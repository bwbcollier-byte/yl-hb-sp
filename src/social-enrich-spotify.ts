import dotenv from 'dotenv';
import fetch from 'node-fetch';
import { supabase } from './supabase';

dotenv.config();

/**
 * SPOTIFY SOCIAL ENRICHER (RapidAPI Version - SUPER BATCHED)
 * 
 * 1. Reads from social_profiles WHERE social_type = 'Spotify' AND status IS NULL
 * 2. Hits RapidAPI sequentially to respect rate limits.
 * 3. Accumulates ALL discoveries (Albums, Events, Related Artists, Social Links).
 * 4. Per batch, performs BULK operations to merge/create records in the DB.
 */

const BATCH_SIZE = 20; // Reduced for stability during complex bulk ops
const SLEEP_MS = 1000;
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

const RAPID_API_KEY = process.env.RAPID_API_KEY || '8f8ab324eamsh88b8de70b402e0cp1d7d0ajsn13c934eadbd9';
const RAPID_API_HOST = 'spotify-data.p.rapidapi.com';

const PLATFORM_MAP: Record<string, string> = {
    'FACEBOOK': 'Facebook',
    'INSTAGRAM': 'Instagram',
    'TWITTER': 'Twitter',
    'X': 'Twitter',
    'WIKIPEDIA': 'Website',
    'YOUTUBE': 'YouTube',
    'SOUNDCLOUD': 'Soundcloud',
    'DEEZER': 'Deezer',
    'TIKTOK': 'TikTok',
    'TIK TOK': 'TikTok',
    'DISCORD': 'Discord',
    'TWITCH': 'Twitch',
    'BANDCAMP': 'Bandcamp',
    'APPLE_MUSIC': 'AppleMusic',
};

async function fetchSpotifyArtistOverview(spotifyId: string): Promise<any> {
    const url = `https://${RAPID_API_HOST}/artist_overview/?id=${spotifyId}`;
    const options: any = {
        headers: {
            'x-rapidapi-host': RAPID_API_HOST,
            'x-rapidapi-key': RAPID_API_KEY,
        }
    };

    try {
        const res = await fetch(url, options);
        if (res.status === 429) {
            console.log(`\n   ⏳ Rate limited on ${RAPID_API_HOST}. Trying next...`);
            return fetchSpotifyArtistOverview(spotifyId);
        }
        if (!res.ok) return null;
        const data: any = await res.json();
        return data.data?.artist || data.data?.artistUnion || null;
    } catch (error) {
        console.error(`\n❌ Network error for ${spotifyId}:`, (error as any).message);
        return null;
    }
}

function extractIdFromUrl(url: string, type: string): string | null {
    if (!url) return null;
    try {
        const u = new URL(url);
        const pathParts = u.pathname.split('/').filter(p => p.length > 0);
        if (['Instagram', 'Twitter', 'Facebook', 'TikTok'].includes(type)) return pathParts[0] || null;
        if (type === 'YouTube') {
            if (['channel', 'user', 'c'].includes(pathParts[0])) return pathParts[1] || null;
            if (pathParts[0]?.startsWith('@')) return pathParts[0];
            return pathParts[0] || null;
        }
    } catch {
        const parts = url.split('/').filter(p => p.length > 0);
        return parts[parts.length - 1] || null;
    }
    return null;
}

function getCleanUsername(name: string): string {
    return name.toLowerCase().replace(/[^a-z0-9]/g, '');
}

// Reuse MusicBrainz linker logic
const USER_AGENT = 'HBTalentMusicProfiles/1.0 (contact@yunikon-labs.com)';
async function fetchMBIDFromSpotifyUrl(spotifyUrl: string): Promise<{mbid: string, name: string} | null> {
    const url = `https://musicbrainz.org/ws/2/url?resource=${encodeURIComponent(spotifyUrl)}&inc=artist-rels&fmt=json`;
    try {
        const res = await fetch(url, {
            headers: { 'User-Agent': USER_AGENT, Accept: 'application/json' },
        });
        if (!res.ok) return null;
        const data = await res.json();
        await sleep(1100); // 1 req/sec limit
        if (data.relations && data.relations.length > 0) {
            const artistRel = data.relations.find((r: any) => r['target-type'] === 'artist' || r.artist);
            if (artistRel && artistRel.artist) {
                return { mbid: artistRel.artist.id, name: artistRel.artist.name };
            }
        }
    } catch {}
    return null;
}

async function processBatch(manualProfiles?: any[]): Promise<number> {
    let profiles: any[] | null = manualProfiles || null;
    
    if (!profiles) {
        const { data, error } = await supabase
            .from('social_profiles')
            .select('id, social_id, talent_id, name, workflow_logs')
            .eq('social_type', 'Spotify')
            .not('status', 'in', '("Done","Error")')
            .not('social_id', 'is', null)
            .neq('social_id', '')
            .limit(BATCH_SIZE);

        if (error) {
            console.error('❌ Error fetching Spotify profiles:', error.message);
            return 0;
        }
        profiles = data;
    }

    if (!profiles || profiles.length === 0) return 0;

    const talentIds = profiles.map(p => p.talent_id);
    const { data: existingSocials } = await supabase.from('social_profiles').select('id, talent_id, social_type, social_id, status').in('talent_id', talentIds);
    const { data: existingTalents } = await supabase.from('talent_profiles').select('id, spotify_id, musicbrainz_id, sp_genres, sp_popularity, workflow_logs').in('id', talentIds);
    
    const socialMap = new Map<string, any>();
    existingSocials?.forEach(s => socialMap.set(`${s.talent_id}_${s.social_type}`, s));
    
    const talentMap = new Map<string, any>();
    existingTalents?.forEach(t => talentMap.set(t.id, t));

    const socialUpdates: any[] = [];
    const mediaInserts: any[] = [];
    const eventInserts: any[] = [];
    const talentUpdates: any[] = [];

    const now = new Date().toISOString();

    for (const profile of profiles) {
        process.stdout.write(`\r   🎵 Spotify: ${profile.name || profile.social_id}...`);
        const currentLogs = Array.isArray(profile.workflow_logs) ? profile.workflow_logs : [];

        const artist = await fetchSpotifyArtistOverview(profile.social_id!);
        if (artist) {
            const apiProfile = artist.profile || {};
            const stats = artist.stats || {};
            const visuals = artist.visuals || {};
            const discography = artist.discography || {};

            const avatarSources = visuals.avatarImage?.sources || [];
            const largestAvatar = [...avatarSources].sort((a: any, b: any) => (b.width || 0) - (a.width || 0))[0];
            const artistName = apiProfile.name || profile.name;
            const cleanUsername = getCleanUsername(artistName);

            // 1. Accumulate Main Profile Update
            socialUpdates.push({
                id: profile.id,
                name: artistName,
                username: cleanUsername,
                is_verified: apiProfile.verified || false,
                social_image: largestAvatar?.url || null,
                social_about: apiProfile.biography?.text ? apiProfile.biography.text.slice(0, 5000) : null,
                followers_count: stats.followers || null,
                following: stats.monthlyListeners || null,
                social_rank: stats.worldRank || null,
                images: visuals.gallery?.items?.map((item: any) => {
                    const sources = item.sources || [];
                    const largest = [...sources].sort((a: any, b: any) => (b.width || 0) - (a.width || 0))[0];
                    return largest?.url;
                }).filter(Boolean) || [],
                top_cities: stats.topCities?.items || [],
                status: 'Done',
                last_checked: now,
                updated_at: now,
                workflow_logs: [...currentLogs, { workflow: "Spotify_Enrichment", date: now, result: "Success - Enriched profile, media, and events", status: "success" }]
            });

            // 2. Accumulate Media (Albums)
            const albums = discography.albums?.items || [];
            albums.forEach((group: any) => {
                group.releases?.items?.forEach((item: any) => {
                    if (item.id) {
                        const coverArt = item.coverArt?.sources || [];
                        const largestCover = [...coverArt].sort((a: any, b: any) => (b.width || 0) - (a.width || 0))[0];
                        mediaInserts.push({
                            talent_id: profile.talent_id,
                            name: item.name,
                            media_type: item.type === 'ALBUM' ? 'Album' : 'Single',
                            spotify_album_id: item.id,
                            media_url: `https://open.spotify.com/album/${item.id}`,
                            image_url: largestCover?.url || null,
                            release_date: item.date ? `${item.date.year}-${String(item.date.month || 1).padStart(2, '0')}-${String(item.date.day || 1).padStart(2, '0')}` : null,
                            label: item.label || null,
                            created_at: now
                        });
                    }
                });
            });

            // 3. Accumulate Events
            const concerts = artist.goods?.events?.concerts?.items || [];
            concerts.forEach((concert: any) => {
                if (concert.id) {
                    eventInserts.push({
                        talent_id: profile.talent_id,
                        name: concert.title || artistName,
                        event_type: concert.category || 'Concert',
                        spotify_id: concert.id,
                        event_url: `https://open.spotify.com/concert/${concert.id}`,
                        venue_name: concert.venue?.name || null,
                        location_name: concert.venue?.location?.name || null,
                        latitude: concert.venue?.location?.coordinates?.latitude || null,
                        longitude: concert.venue?.location?.coordinates?.longitude || null,
                        start_date: concert.date?.isoString || null,
                        created_at: now
                    });
                }
            });

            const talent = talentMap.get(profile.talent_id);
            if (talent) {
                const talentLogs = Array.isArray(talent.workflow_logs) ? talent.workflow_logs : [];
                const talentUpdate: any = { 
                    id: talent.id,
                    updated_at: now,
                    workflow_logs: [...talentLogs, { workflow: "Spotify_Metadata_Enrichment", date: now, result: "Success - Updated genres, popularity, and stats", status: "success" }]
                };
                
                if (artist.genres) {
                    const genreList = Array.isArray(artist.genres) ? artist.genres : (artist.genres.genres || []);
                    talentUpdate.sp_genres = Array.isArray(genreList) ? genreList.join(', ') : genreList;
                }
                if (artist.popularity) talentUpdate.sp_popularity = artist.popularity;
                if (!talent.sp_image && largestAvatar?.url) talentUpdate.sp_image = largestAvatar.url;
                
                // Add stats
                if (stats.followers) talentUpdate.sp_followers = stats.followers;
                if (stats.monthlyListeners) talentUpdate.sp_monthly_listeners = stats.monthlyListeners;
                
                talentUpdates.push(talentUpdate);
            }

            // 5. Accumulate Social Links
            const externalLinks = apiProfile.externalLinks?.items || [];
            externalLinks.forEach((link: any) => {
                const sType = PLATFORM_MAP[link.name?.toUpperCase()] || (link.name ? 'Website' : null);
                if (sType && link.url) {
                    const existing = socialMap.get(`${profile.talent_id}_${sType}`);
                    if (!existing) {
                        socialUpdates.push({
                            talent_id: profile.talent_id,
                            social_type: sType,
                            social_id: extractIdFromUrl(link.url, sType),
                            name: artistName,
                            social_url: link.url,
                            status: null,
                            linking_status: 'done',
                            created_at: now,
                            updated_at: now
                        });
                    } else if (!existing.social_id || existing.status !== 'Done') {
                        const extractedId = extractIdFromUrl(link.url, sType);
                        if (extractedId) {
                            socialUpdates.push({
                                id: existing.id,
                                social_id: extractedId,
                                social_url: link.url,
                                updated_at: now
                            });
                        }
                    }
                }
            });

            // 6. Integrate MusicBrainz Linker
            if (talent && !talent.musicbrainz_id) {
                const spotifyUrl = `https://open.spotify.com/artist/${profile.social_id}`;
                const mbData = await fetchMBIDFromSpotifyUrl(spotifyUrl);
                if (mbData) {
                    // Update talent profile with MBID
                    const existingUpdate = talentUpdates.find(u => u.id === talent.id);
                    if (existingUpdate) {
                        existingUpdate.musicbrainz_id = mbData.mbid;
                        const logs = Array.isArray(existingUpdate.workflow_logs) ? existingUpdate.workflow_logs : [];
                        existingUpdate.workflow_logs = [...logs, { workflow: "MusicBrainz_Spotify_Linker", date: now, result: `Success - Found MBID ${mbData.mbid}`, status: "success" }];
                    } else {
                        const talentLogs = talent.workflow_logs || [];
                        talentUpdates.push({ 
                            id: talent.id, 
                            musicbrainz_id: mbData.mbid, 
                            updated_at: now, 
                            workflow_logs: [...talentLogs, { workflow: "MusicBrainz_Spotify_Linker", date: now, result: `Success - Found MBID ${mbData.mbid}`, status: "success" }] 
                        });
                    }
                    
                    // Add MusicBrainz profile if not exists
                    const existingMB = socialMap.get(`${profile.talent_id}_MusicBrainz`);
                    if (!existingMB) {
                        const cleanMBUsername = mbData.name.toLowerCase().replace(/[^a-z0-9]/g, '');
                        socialUpdates.push({
                            talent_id: profile.talent_id,
                            social_type: 'MusicBrainz',
                            social_id: mbData.mbid,
                            name: mbData.name,
                            username: cleanMBUsername,
                            social_url: `https://musicbrainz.org/artist/${mbData.mbid}`,
                            status: null,
                            linking_status: 'done',
                            created_at: now,
                            updated_at: now
                        });
                    }
                }
            }

        } else {
            socialUpdates.push({ 
                id: profile.id, 
                status: 'Error', 
                last_checked: now,
                workflow_logs: [...currentLogs, { workflow: "Spotify_Enrichment", date: now, result: "Failed - Unexpected API response", status: "error" }]
            });
        }
        await sleep(SLEEP_MS);
    }

    // 🔥 EXECUTE BULK OPERATIONS
    if (socialUpdates.length > 0) await supabase.from('social_profiles').upsert(socialUpdates);
    if (mediaInserts.length > 0) await supabase.from('media_profiles').upsert(mediaInserts, { onConflict: 'spotify_album_id' });
    if (eventInserts.length > 0) await supabase.from('event_profiles').upsert(eventInserts, { onConflict: 'spotify_id' });
    if (talentUpdates.length > 0) await supabase.from('talent_profiles').upsert(talentUpdates);

    console.log(`\n   ✅ Batched: ${socialUpdates.length} Socials, ${mediaInserts.length} Media, ${eventInserts.length} Events, ${talentUpdates.length} Talent Updates.`);
    return profiles.length;
}

async function processSpecific(queries: string[]) {
    console.log(`🔍 Processing specific queries: ${queries.join(', ')}`);
    for (const query of queries) {
        let isUrl = query.startsWith('http');
        let socialId = isUrl ? query.split('/').pop()?.split('?')[0] : query;
        
        const { data: profiles, error } = await supabase
            .from('social_profiles')
            .select('*')
            .eq('social_type', 'Spotify')
            .eq('social_id', socialId);
            
        if (error || !profiles || profiles.length === 0) {
            console.log(`❌ No Spotify social_profile found for ID: ${socialId}`);
            continue;
        }
        
        // We can process a single-item batch or refactor processBatch. 
        // For simplicity, we just reuse the core loop logic or refactor.
        // Let's refactor the batch processor slightly to accept a pre-fetched list.
        await processBatch(profiles);
    }
}

async function main() {
    console.log('\n🎵 Spotify Super-Enricher (SUPER BATCHED)');
    console.log('==========================================');

    // Parse CLI arguments
    const args = process.argv.slice(2);
    if (args.length > 0) {
        let targets = [];
        for (let i = 0; i < args.length; i++) {
            if (args[i] === '--id' || args[i] === '--url') {
                if (args[i+1]) {
                    targets.push(args[i+1]);
                    i++;
                }
            } else if (!args[i].startsWith('--')) {
                targets.push(args[i]);
            }
        }
        
        if (targets.length > 0) {
            await processSpecific(targets);
            console.log('\n✨ Done processing specific records!');
            return;
        }
    }

    const { count: total } = await supabase
        .from('social_profiles')
        .select('id', { count: 'estimated', head: true })
        .eq('social_type', 'Spotify')
        .not('status', 'in', '("Done","Error")');

    console.log(`📊 Spotify profiles to process: ~${total || 0}`);

    let totalProcessed = 0;
    while (true) {
        const count = await processBatch();
        if (count === 0) break;
        totalProcessed += count;
        process.stdout.write(`\r   ✅ Total processed: ${totalProcessed}`);
    }

    console.log(`\n\n✨ Done! Enriched ${totalProcessed} Spotify profiles.`);
}

main().catch(err => {
    console.error('Fatal error:', err);
    process.exit(1);
});

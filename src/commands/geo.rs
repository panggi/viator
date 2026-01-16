//! Geospatial command implementations.
//!
//! Redis geospatial commands store locations and perform distance/radius queries.
//! Internally, geo data is stored as sorted sets with geohash scores.

use super::ParsedCommand;
use crate::Result;
use crate::error::CommandError;
use crate::protocol::Frame;
use crate::server::ClientState;
use crate::storage::Db;
use crate::types::{Key, ViatorValue};
use bytes::Bytes;
use std::f64::consts::PI;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

/// Earth radius in meters
const EARTH_RADIUS_M: f64 = 6372797.560856;

/// Encode latitude/longitude to geohash as f64 score
fn geohash_encode(lat: f64, lon: f64) -> f64 {
    // Normalize coordinates
    let lat = lat.clamp(-85.05112878, 85.05112878);
    let lon = lon.clamp(-180.0, 180.0);

    // Convert to range [0, 1]
    let lat_offset = (lat + 90.0) / 180.0;
    let lon_offset = (lon + 180.0) / 360.0;

    // Interleave bits (simplified - real Redis uses 52-bit precision)
    let lat_bits = (lat_offset * (1u64 << 26) as f64) as u64;
    let lon_bits = (lon_offset * (1u64 << 26) as f64) as u64;

    let mut hash: u64 = 0;
    for i in 0..26 {
        hash |= ((lon_bits >> (25 - i)) & 1) << (51 - i * 2);
        hash |= ((lat_bits >> (25 - i)) & 1) << (50 - i * 2);
    }

    hash as f64
}

/// Decode geohash score to latitude/longitude
fn geohash_decode(hash: f64) -> (f64, f64) {
    let hash = hash as u64;

    let mut lat_bits: u64 = 0;
    let mut lon_bits: u64 = 0;

    for i in 0..26 {
        lon_bits |= ((hash >> (51 - i * 2)) & 1) << (25 - i);
        lat_bits |= ((hash >> (50 - i * 2)) & 1) << (25 - i);
    }

    let lat = (lat_bits as f64 / (1u64 << 26) as f64) * 180.0 - 90.0;
    let lon = (lon_bits as f64 / (1u64 << 26) as f64) * 360.0 - 180.0;

    (lat, lon)
}

/// Calculate distance between two points using Haversine formula
fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1 * PI / 180.0;
    let lat2_rad = lat2 * PI / 180.0;
    let lon1_rad = lon1 * PI / 180.0;
    let lon2_rad = lon2 * PI / 180.0;

    let dlat = lat2_rad - lat1_rad;
    let dlon = lon2_rad - lon1_rad;

    let a =
        (dlat / 2.0).sin().powi(2) + lat1_rad.cos() * lat2_rad.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

/// Convert distance to specified unit
fn convert_distance(meters: f64, unit: &str) -> f64 {
    match unit.to_lowercase().as_str() {
        "m" => meters,
        "km" => meters / 1000.0,
        "mi" => meters / 1609.344,
        "ft" => meters / 0.3048,
        _ => meters,
    }
}

/// Convert geohash to base32 string representation
fn geohash_to_string(hash: f64) -> String {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let hash = hash as u64;
    let mut result = String::with_capacity(11);

    for i in (0..11).rev() {
        let idx = ((hash >> (i * 5)) & 0x1F) as usize;
        result.push(ALPHABET[idx] as char);
    }

    result
}

/// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
pub fn cmd_geoadd(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let key = Key::from(cmd.args[0].clone());
        let mut idx = 1;
        let mut nx = false;
        let mut xx = false;
        let mut ch = false;

        // Parse options
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "NX" => {
                    nx = true;
                    idx += 1;
                }
                "XX" => {
                    xx = true;
                    idx += 1;
                }
                "CH" => {
                    ch = true;
                    idx += 1;
                }
                _ => break,
            }
        }

        // Remaining args should be longitude, latitude, member triplets
        if (cmd.args.len() - idx) % 3 != 0 || cmd.args.len() - idx < 3 {
            return Err(CommandError::WrongArity {
                command: "GEOADD".to_string(),
            }
            .into());
        }

        let value = db.get(&key).unwrap_or_else(ViatorValue::new_zset);
        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let mut guard = zset.write();

        let mut added = 0i64;
        let mut changed = 0i64;

        while idx + 2 < cmd.args.len() {
            let lon: f64 = cmd.get_f64(idx)?;
            let lat: f64 = cmd.get_f64(idx + 1)?;
            let member = cmd.args[idx + 2].clone();

            // Validate coordinates
            if !(-180.0..=180.0).contains(&lon) || !(-85.05112878..=85.05112878).contains(&lat) {
                return Err(CommandError::InvalidArgument {
                    command: "GEOADD".to_string(),
                    arg: format!("{},{}", lon, lat),
                }
                .into());
            }

            let score = geohash_encode(lat, lon);
            let existed = guard.score(&member).is_some();

            if (nx && existed) || (xx && !existed) {
                idx += 3;
                continue;
            }

            let old_score = guard.score(&member);
            guard.add(member, score);

            if !existed {
                added += 1;
            } else if ch && old_score != Some(score) {
                changed += 1;
            }

            idx += 3;
        }

        drop(guard);
        db.set(key, value);

        Ok(Frame::Integer(if ch { added + changed } else { added }))
    })
}

/// GEODIST key member1 member2 [M|KM|FT|MI]
pub fn cmd_geodist(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());
        let member1 = &cmd.args[1];
        let member2 = &cmd.args[2];
        let unit = if cmd.args.len() > 3 {
            cmd.get_str(3)?
        } else {
            "m"
        };

        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Null),
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        let score1 = match guard.score(member1) {
            Some(s) => s,
            None => return Ok(Frame::Null),
        };
        let score2 = match guard.score(member2) {
            Some(s) => s,
            None => return Ok(Frame::Null),
        };

        let (lat1, lon1) = geohash_decode(score1);
        let (lat2, lon2) = geohash_decode(score2);

        let dist = haversine_distance(lat1, lon1, lat2, lon2);
        let converted = convert_distance(dist, unit);

        Ok(Frame::Bulk(Bytes::from(format!("{:.4}", converted))))
    })
}

/// GEOHASH key member [member ...]
pub fn cmd_geohash(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        let value = match db.get(&key) {
            Some(v) => v,
            None => {
                return Ok(Frame::Array(
                    cmd.args[1..].iter().map(|_| Frame::Null).collect(),
                ));
            }
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|member| match guard.score(member) {
                Some(score) => Frame::Bulk(Bytes::from(geohash_to_string(score))),
                None => Frame::Null,
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// GEOPOS key member [member ...]
pub fn cmd_geopos(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(2)?;

        let key = Key::from(cmd.args[0].clone());

        let value = match db.get(&key) {
            Some(v) => v,
            None => {
                return Ok(Frame::Array(
                    cmd.args[1..].iter().map(|_| Frame::Null).collect(),
                ));
            }
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        let results: Vec<Frame> = cmd.args[1..]
            .iter()
            .map(|member| match guard.score(member) {
                Some(score) => {
                    let (lat, lon) = geohash_decode(score);
                    Frame::Array(vec![
                        Frame::Bulk(Bytes::from(format!("{:.6}", lon))),
                        Frame::Bulk(Bytes::from(format!("{:.6}", lat))),
                    ])
                }
                None => Frame::Null,
            })
            .collect();

        Ok(Frame::Array(results))
    })
}

/// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
pub fn cmd_georadius(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;

        let key = Key::from(cmd.args[0].clone());
        let lon: f64 = cmd.get_f64(1)?;
        let lat: f64 = cmd.get_f64(2)?;
        let radius: f64 = cmd.get_f64(3)?;
        let unit = cmd.get_str(4)?;

        // Parse options
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count: Option<usize> = None;
        let mut asc = true;

        let mut idx = 5;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "WITHCOORD" => withcoord = true,
                "WITHDIST" => withdist = true,
                "WITHHASH" => withhash = true,
                "ASC" => asc = true,
                "DESC" => asc = false,
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                }
                _ => {}
            }
            idx += 1;
        }

        let radius_m = match unit.to_lowercase().as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        let mut results: Vec<(f64, Bytes, f64)> = vec![];

        // Check all members for distance
        for entry in guard.iter() {
            let (member_lat, member_lon) = geohash_decode(entry.score);
            let dist = haversine_distance(lat, lon, member_lat, member_lon);

            if dist <= radius_m {
                results.push((dist, entry.member.clone(), entry.score));
            }
        }

        // Sort by distance
        if asc {
            results.sort_by(|a, b| a.0.total_cmp(&b.0));
        } else {
            results.sort_by(|a, b| b.0.total_cmp(&a.0));
        }

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        // Format results
        let frames: Vec<Frame> = results
            .into_iter()
            .map(|(dist, member, score)| {
                if !withcoord && !withdist && !withhash {
                    Frame::Bulk(member)
                } else {
                    let mut arr = vec![Frame::Bulk(member)];
                    if withdist {
                        arr.push(Frame::Bulk(Bytes::from(format!(
                            "{:.4}",
                            convert_distance(dist, unit)
                        ))));
                    }
                    if withhash {
                        arr.push(Frame::Integer(score as i64));
                    }
                    if withcoord {
                        let (lat, lon) = geohash_decode(score);
                        arr.push(Frame::Array(vec![
                            Frame::Bulk(Bytes::from(format!("{:.6}", lon))),
                            Frame::Bulk(Bytes::from(format!("{:.6}", lat))),
                        ]));
                    }
                    Frame::Array(arr)
                }
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// GEOSEARCH key [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI] [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
pub fn cmd_geosearch(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(3)?;

        let key = Key::from(cmd.args[0].clone());

        // Parse options
        let mut from_member: Option<Bytes> = None;
        let mut from_lonlat: Option<(f64, f64)> = None;
        let mut by_radius: Option<(f64, String)> = None;
        let mut by_box: Option<(f64, f64, String)> = None;
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count: Option<usize> = None;
        let mut asc = true;

        let mut idx = 1;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "FROMMEMBER" => {
                    idx += 1;
                    from_member = Some(
                        cmd.args
                            .get(idx)
                            .cloned()
                            .ok_or(CommandError::SyntaxError)?,
                    );
                }
                "FROMLONLAT" => {
                    idx += 1;
                    let lon = cmd.get_f64(idx)?;
                    idx += 1;
                    let lat = cmd.get_f64(idx)?;
                    from_lonlat = Some((lon, lat));
                }
                "BYRADIUS" => {
                    idx += 1;
                    let radius = cmd.get_f64(idx)?;
                    idx += 1;
                    let unit = cmd.get_str(idx)?.to_string();
                    by_radius = Some((radius, unit));
                }
                "BYBOX" => {
                    idx += 1;
                    let width = cmd.get_f64(idx)?;
                    idx += 1;
                    let height = cmd.get_f64(idx)?;
                    idx += 1;
                    let unit = cmd.get_str(idx)?.to_string();
                    by_box = Some((width, height, unit));
                }
                "WITHCOORD" => withcoord = true,
                "WITHDIST" => withdist = true,
                "WITHHASH" => withhash = true,
                "ASC" => asc = true,
                "DESC" => asc = false,
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                }
                "ANY" => {} // Used with COUNT, we just accept it
                _ => {}
            }
            idx += 1;
        }

        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        // Get center point
        let (center_lon, center_lat) = if let Some(ref member) = from_member {
            let score = guard.score(member).ok_or(CommandError::SyntaxError)?;
            let (lat, lon) = geohash_decode(score);
            (lon, lat)
        } else if let Some((lon, lat)) = from_lonlat {
            (lon, lat)
        } else {
            return Err(CommandError::SyntaxError.into());
        };

        let mut results: Vec<(f64, Bytes, f64)> = vec![];

        // Get radius in meters
        let radius_m = if let Some((radius, ref unit)) = by_radius {
            match unit.to_lowercase().as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            }
        } else if let Some((width, height, ref unit)) = by_box {
            // Use diagonal for box search approximation
            let w = match unit.to_lowercase().as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            };
            let h = match unit.to_lowercase().as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            };
            (w * w + h * h).sqrt() / 2.0
        } else {
            return Err(CommandError::SyntaxError.into());
        };

        // Check all members for distance
        for entry in guard.iter() {
            let (member_lat, member_lon) = geohash_decode(entry.score);
            let dist = haversine_distance(center_lat, center_lon, member_lat, member_lon);

            if dist <= radius_m {
                results.push((dist, entry.member.clone(), entry.score));
            }
        }

        // Sort by distance
        if asc {
            results.sort_by(|a, b| a.0.total_cmp(&b.0));
        } else {
            results.sort_by(|a, b| b.0.total_cmp(&a.0));
        }

        // Apply count limit
        if let Some(c) = count {
            results.truncate(c);
        }

        // Determine unit for output
        let unit = by_radius
            .as_ref()
            .map(|(_, u)| u.as_str())
            .or(by_box.as_ref().map(|(_, _, u)| u.as_str()))
            .unwrap_or("m");

        // Format results
        let frames: Vec<Frame> = results
            .into_iter()
            .map(|(dist, member, score)| {
                if !withcoord && !withdist && !withhash {
                    Frame::Bulk(member)
                } else {
                    let mut arr = vec![Frame::Bulk(member)];
                    if withdist {
                        arr.push(Frame::Bulk(Bytes::from(format!(
                            "{:.4}",
                            convert_distance(dist, unit)
                        ))));
                    }
                    if withhash {
                        arr.push(Frame::Integer(score as i64));
                    }
                    if withcoord {
                        let (lat, lon) = geohash_decode(score);
                        arr.push(Frame::Array(vec![
                            Frame::Bulk(Bytes::from(format!("{:.6}", lon))),
                            Frame::Bulk(Bytes::from(format!("{:.6}", lat))),
                        ]));
                    }
                    Frame::Array(arr)
                }
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
pub fn cmd_georadiusbymember(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(4)?;

        let key = Key::from(cmd.args[0].clone());
        let member = &cmd.args[1];
        let radius: f64 = cmd.get_f64(2)?;
        let unit = cmd.get_str(3)?;

        // Get member's position first
        let value = match db.get(&key) {
            Some(v) => v,
            None => return Ok(Frame::Array(vec![])),
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        let score = match guard.score(member) {
            Some(s) => s,
            None => return Ok(Frame::Array(vec![])),
        };

        let (center_lat, center_lon) = geohash_decode(score);

        // Parse options
        let mut withcoord = false;
        let mut withdist = false;
        let mut withhash = false;
        let mut count: Option<usize> = None;
        let mut asc = true;

        let mut idx = 4;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "WITHCOORD" => withcoord = true,
                "WITHDIST" => withdist = true,
                "WITHHASH" => withhash = true,
                "ASC" => asc = true,
                "DESC" => asc = false,
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                }
                _ => {}
            }
            idx += 1;
        }

        let radius_m = match unit.to_lowercase().as_str() {
            "m" => radius,
            "km" => radius * 1000.0,
            "mi" => radius * 1609.344,
            "ft" => radius * 0.3048,
            _ => return Err(CommandError::SyntaxError.into()),
        };

        let mut results: Vec<(f64, Bytes, f64)> = vec![];

        for entry in guard.iter() {
            let (member_lat, member_lon) = geohash_decode(entry.score);
            let dist = haversine_distance(center_lat, center_lon, member_lat, member_lon);

            if dist <= radius_m {
                results.push((dist, entry.member.clone(), entry.score));
            }
        }

        if asc {
            results.sort_by(|a, b| a.0.total_cmp(&b.0));
        } else {
            results.sort_by(|a, b| b.0.total_cmp(&a.0));
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        let frames: Vec<Frame> = results
            .into_iter()
            .map(|(dist, member, score)| {
                if !withcoord && !withdist && !withhash {
                    Frame::Bulk(member)
                } else {
                    let mut arr = vec![Frame::Bulk(member)];
                    if withdist {
                        arr.push(Frame::Bulk(Bytes::from(format!(
                            "{:.4}",
                            convert_distance(dist, unit)
                        ))));
                    }
                    if withhash {
                        arr.push(Frame::Integer(score as i64));
                    }
                    if withcoord {
                        let (lat, lon) = geohash_decode(score);
                        arr.push(Frame::Array(vec![
                            Frame::Bulk(Bytes::from(format!("{:.6}", lon))),
                            Frame::Bulk(Bytes::from(format!("{:.6}", lat))),
                        ]));
                    }
                    Frame::Array(arr)
                }
            })
            .collect();

        Ok(Frame::Array(frames))
    })
}

/// GEOSEARCHSTORE destination source [FROMMEMBER member | FROMLONLAT lon lat] [BYRADIUS radius M|KM|FT|MI | BYBOX width height M|KM|FT|MI] [ASC|DESC] [COUNT count [ANY]] [STOREDIST]
pub fn cmd_geosearchstore(
    cmd: ParsedCommand,
    db: Arc<Db>,
    _client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    Box::pin(async move {
        cmd.require_args(5)?;

        let dest_key = Key::from(cmd.args[0].clone());
        let src_key = Key::from(cmd.args[1].clone());

        // Similar parsing as GEOSEARCH but store results
        let mut from_member: Option<Bytes> = None;
        let mut from_lonlat: Option<(f64, f64)> = None;
        let mut by_radius: Option<(f64, String)> = None;
        let mut by_box: Option<(f64, f64, String)> = None;
        let mut count: Option<usize> = None;
        let mut asc = true;
        let mut storedist = false;

        let mut idx = 2;
        while idx < cmd.args.len() {
            let arg = cmd.get_str(idx)?.to_uppercase();
            match arg.as_str() {
                "FROMMEMBER" => {
                    idx += 1;
                    from_member = Some(
                        cmd.args
                            .get(idx)
                            .cloned()
                            .ok_or(CommandError::SyntaxError)?,
                    );
                }
                "FROMLONLAT" => {
                    idx += 1;
                    let lon = cmd.get_f64(idx)?;
                    idx += 1;
                    let lat = cmd.get_f64(idx)?;
                    from_lonlat = Some((lon, lat));
                }
                "BYRADIUS" => {
                    idx += 1;
                    let radius = cmd.get_f64(idx)?;
                    idx += 1;
                    let unit = cmd.get_str(idx)?.to_string();
                    by_radius = Some((radius, unit));
                }
                "BYBOX" => {
                    idx += 1;
                    let width = cmd.get_f64(idx)?;
                    idx += 1;
                    let height = cmd.get_f64(idx)?;
                    idx += 1;
                    let unit = cmd.get_str(idx)?.to_string();
                    by_box = Some((width, height, unit));
                }
                "ASC" => asc = true,
                "DESC" => asc = false,
                "COUNT" => {
                    idx += 1;
                    count = Some(cmd.get_u64(idx)? as usize);
                }
                "STOREDIST" => storedist = true,
                "ANY" => {}
                _ => {}
            }
            idx += 1;
        }

        let value = match db.get(&src_key) {
            Some(v) => v,
            None => return Ok(Frame::Integer(0)),
        };

        let zset = value.as_zset().ok_or(CommandError::WrongType)?;
        let guard = zset.read();

        // Get center point
        let (center_lon, center_lat) = if let Some(ref member) = from_member {
            let score = guard.score(member).ok_or(CommandError::SyntaxError)?;
            let (lat, lon) = geohash_decode(score);
            (lon, lat)
        } else if let Some((lon, lat)) = from_lonlat {
            (lon, lat)
        } else {
            return Err(CommandError::SyntaxError.into());
        };

        let radius_m = if let Some((radius, ref unit)) = by_radius {
            match unit.to_lowercase().as_str() {
                "m" => radius,
                "km" => radius * 1000.0,
                "mi" => radius * 1609.344,
                "ft" => radius * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            }
        } else if let Some((width, height, ref unit)) = by_box {
            let w = match unit.to_lowercase().as_str() {
                "m" => width,
                "km" => width * 1000.0,
                "mi" => width * 1609.344,
                "ft" => width * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            };
            let h = match unit.to_lowercase().as_str() {
                "m" => height,
                "km" => height * 1000.0,
                "mi" => height * 1609.344,
                "ft" => height * 0.3048,
                _ => return Err(CommandError::SyntaxError.into()),
            };
            (w * w + h * h).sqrt() / 2.0
        } else {
            return Err(CommandError::SyntaxError.into());
        };

        let mut results: Vec<(f64, Bytes, f64)> = vec![];

        for entry in guard.iter() {
            let (member_lat, member_lon) = geohash_decode(entry.score);
            let dist = haversine_distance(center_lat, center_lon, member_lat, member_lon);

            if dist <= radius_m {
                results.push((dist, entry.member.clone(), entry.score));
            }
        }

        if asc {
            results.sort_by(|a, b| a.0.total_cmp(&b.0));
        } else {
            results.sort_by(|a, b| b.0.total_cmp(&a.0));
        }

        if let Some(c) = count {
            results.truncate(c);
        }

        drop(guard);

        // Store results
        let dest_value = ViatorValue::new_zset();
        let dest_zset = dest_value
            .as_zset()
            .unwrap_or_else(|| unreachable!("type guaranteed by get_or_create_zset"));
        let mut dest_guard = dest_zset.write();

        for (dist, member, score) in &results {
            let store_score = if storedist { *dist } else { *score };
            dest_guard.add(member.clone(), store_score);
        }

        let count = results.len();
        drop(dest_guard);
        db.set(dest_key, dest_value);

        Ok(Frame::Integer(count as i64))
    })
}

/// GEORADIUS_RO key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
/// Read-only variant of GEORADIUS (no STORE/STOREDIST options)
pub fn cmd_georadius_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    // Read-only version is identical to regular GEORADIUS for querying
    // (the _RO variant just doesn't support STORE options)
    cmd_georadius(cmd, db, client)
}

/// GEORADIUSBYMEMBER_RO key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count] [ASC|DESC]
/// Read-only variant of GEORADIUSBYMEMBER (no STORE/STOREDIST options)
pub fn cmd_georadiusbymember_ro(
    cmd: ParsedCommand,
    db: Arc<Db>,
    client: Arc<ClientState>,
) -> Pin<Box<dyn Future<Output = Result<Frame>> + Send>> {
    // Read-only version is identical to regular GEORADIUSBYMEMBER for querying
    cmd_georadiusbymember(cmd, db, client)
}

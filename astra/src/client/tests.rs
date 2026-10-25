use super::*;
use crate::pack::{ObjType, encode_entry_header, pack_header};
use crate::zlib::{Deflater, compress_bound};

fn config(path: &Path) -> Config {
    let mut config = Config::new("http://cache.test".to_owned(), path.to_owned());
    config.max_objects = 4096;
    config.max_object_bytes = 1 << 20;
    config.max_download_bytes = 64 << 20;
    config
}

fn entry(kind: ObjType, data: &[u8]) -> Vec<u8> {
    let mut z = vec![0; compress_bound(data.len())];
    let n = Deflater::new(1).deflate(data, &mut z).unwrap();
    let mut header = [0; 10];
    let h = encode_entry_header(kind, data.len() as u64, &mut header);
    let mut bytes = header[..h].to_vec();
    bytes.extend_from_slice(&z[..n]);
    bytes
}

fn pack(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut bytes = pack_header(entries.len() as u32).to_vec();
    for entry in entries {
        bytes.extend_from_slice(entry);
    }
    let hash = crate::sha1_parts(&[&bytes]);
    bytes.extend_from_slice(&hash);
    bytes
}

fn response(pack: &[u8], start: u64, end: u64) -> (ResponseMeta, Vec<u8>) {
    let meta = ResponseMeta {
        etag: Oid20(crate::sha1_parts(&[&start.to_le_bytes(), pack])),
        start: Some(start),
        end: Some(end),
        baseline: start == 0,
        tip: None,
    };
    let mut body = crate::gpre::gpre_file_header(1).to_vec();
    body.extend_from_slice(&crate::gpre::gpre_pack_header(
        end as i64,
        pack.len() as i64,
        -1,
    ));
    body.extend_from_slice(pack);
    (meta, body)
}

fn apply(cache: &mut Cache, objects: &[&[u8]], end: u64) -> SyncReport {
    let entries: Vec<_> = objects.iter().map(|b| entry(ObjType::Blob, b)).collect();
    let pack = pack(&entries);
    let (meta, body) = response(&pack, cache.cursor(), end);
    cache
        .apply_response(200, meta, &mut body.as_slice())
        .unwrap()
}

fn contents(cache: &Cache, oid: Oid20) -> Vec<u8> {
    let kind = git_command(cache.cache_dir())
        .args(["cat-file", "-t"])
        .arg(oid.to_string())
        .output()
        .unwrap();
    assert!(kind.status.success());
    let kind = std::str::from_utf8(kind.stdout.trim_ascii()).unwrap();
    let out = git_command(cache.cache_dir())
        .args(["cat-file", kind])
        .arg(oid.to_string())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    out.stdout
}

fn pack_files(path: &Path) -> Vec<PathBuf> {
    let mut files: Vec<_> = fs::read_dir(path.join("objects/pack"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "pack"))
        .collect();
    files.sort();
    files
}

#[test]
fn pinned_baseline_and_two_pack_bound_with_unmanaged_data_and_midx() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    apply(&mut cache, &[b"anchor"], 100);
    let anchor = cache.state.anchor.unwrap();
    let anchor_path = cache.path(Role::Anchor, anchor, "pack");
    let original = fs::read(&anchor_path).unwrap();
    apply(&mut cache, &[b"delta-1", b"anchor"], 101);
    let first_delta = cache.state.delta.unwrap();
    let user_pack = temp.path().join("objects/pack/pack-user.pack");
    fs::write(&user_pack, pack(&[entry(ObjType::Blob, b"unmanaged")])).unwrap();
    checked(
        git_command(temp.path())
            .args(["index-pack"])
            .arg(&user_pack),
    )
    .unwrap();
    checked(git_command(temp.path()).args(["multi-pack-index", "write"])).unwrap();
    apply(&mut cache, &[b"delta-2", b"delta-1"], 102);
    assert_eq!(cache.managed_pack_count(), 2);
    assert_eq!(pack_files(temp.path()).len(), 3);
    assert!(!cache.path(Role::Delta, first_delta, "pack").exists());
    assert!(user_pack.exists());
    assert_eq!(fs::read(&anchor_path).unwrap(), original);
    for bytes in [b"anchor".as_slice(), b"delta-1", b"delta-2", b"unmanaged"] {
        assert_eq!(
            contents(&cache, crate::object_id(ObjType::Blob, bytes)),
            bytes
        );
    }
    checked(git_command(temp.path()).args(["multi-pack-index", "verify"])).unwrap();
    let before = cache.state.delta;
    let report = apply(&mut cache, &[b"anchor", b"delta-1"], 103);
    assert_eq!(report.local_pack_bytes_written, 0);
    assert_eq!(cache.state.delta, before);
    drop(cache);
    let cache = Cache::open(config(temp.path())).unwrap();
    assert_eq!(cache.cursor(), 103);
    assert_eq!(cache.managed_pack_count(), 2);
    assert!(user_pack.exists());
}

#[test]
fn unchanged_response_and_invalid_download_never_rewrite_or_prune() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    apply(&mut cache, &[b"anchor"], 100);
    let meta = ResponseMeta {
        etag: Oid20([7; 20]),
        start: None,
        end: None,
        baseline: false,
        tip: None,
    };
    cache
        .apply_response(200, meta, &mut crate::gpre::gpre_file_header(0).as_slice())
        .unwrap();
    let saved = fs::read(cache.metadata().join("state.bin")).unwrap();
    let report = cache
        .apply_response(304, meta, &mut b"".as_slice())
        .unwrap();
    assert!(report.not_modified);
    assert_eq!(report.downloaded_bytes, 0);
    assert_eq!(fs::read(cache.metadata().join("state.bin")).unwrap(), saved);
    let p = pack(&[entry(ObjType::Blob, b"new")]);
    let (meta, mut bytes) = response(&p, 100, 101);
    bytes.pop();
    assert!(
        cache
            .apply_response(200, meta, &mut bytes.as_slice())
            .is_err()
    );
    assert_eq!(fs::read(cache.metadata().join("state.bin")).unwrap(), saved);
    assert_eq!(cache.cursor(), 100);
    assert_eq!(pack_files(temp.path()).len(), 1);
    assert_eq!(
        contents(&cache, crate::object_id(ObjType::Blob, b"anchor")),
        b"anchor"
    );
}

#[test]
fn local_splice_keeps_ref_bases_even_when_the_anchor_also_has_them() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    let blob_a = crate::object_id(ObjType::Blob, b"a");
    let blob_b = crate::object_id(ObjType::Blob, b"b");
    let mut a = b"100644 file\0".to_vec();
    a.extend_from_slice(&blob_a.0);
    let mut b = b"100644 file\0".to_vec();
    b.extend_from_slice(&blob_b.0);
    let base = crate::object_id(ObjType::Tree, &a);
    let target = crate::object_id(ObjType::Tree, &b);
    let initial = pack(&[entry(ObjType::Blob, b"a"), entry(ObjType::Tree, &a)]);
    let (meta, body) = response(&initial, 0, 100);
    cache
        .apply_response(200, meta, &mut body.as_slice())
        .unwrap();
    let mut instructions = vec![0; 256];
    let len = crate::tree_delta::encode(&a, &b, &mut instructions).unwrap();
    let delta = entry(ObjType::RefDelta, &instructions[..len]);
    let h = crate::pack::parse_entry_header(&entry(ObjType::Tree, &instructions[..len]))
        .unwrap()
        .unwrap()
        .header_len;
    let mut raw = delta[..h].to_vec();
    raw.extend_from_slice(&base.0);
    raw.extend_from_slice(&delta[h..]);
    let incoming = pack(&[entry(ObjType::Tree, &a), raw, entry(ObjType::Blob, b"b")]);
    let (meta, body) = response(&incoming, 100, 101);
    cache
        .apply_response(200, meta, &mut body.as_slice())
        .unwrap();
    assert_eq!(cache.state.delta.unwrap().count, 3);
    assert_eq!(contents(&cache, target), b);
    apply(&mut cache, &[b"later"], 102);
    assert_eq!(contents(&cache, target), b);
    assert_eq!(cache.managed_pack_count(), 2);
}

#[test]
fn interrupted_install_is_recovered_before_retiring_the_predecessor() {
    for point in [
        Failure::Journal,
        Failure::Pack,
        Failure::State,
        Failure::Retire,
    ] {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = Cache::open(config(temp.path())).unwrap();
        apply(&mut cache, &[b"anchor"], 100);
        apply(&mut cache, &[b"old-delta"], 101);
        let old = cache.state.delta.unwrap();
        let old_path = cache.path(Role::Delta, old, "pack");
        let p = pack(&[entry(ObjType::Blob, b"new-delta")]);
        let (meta, body) = response(&p, 101, 102);
        cache.failure = Some(point);
        assert!(
            cache
                .apply_response(200, meta, &mut body.as_slice())
                .is_err()
        );
        assert!(
            cache.failure.is_none(),
            "did not reach the requested interruption point"
        );
        assert!(
            old_path.exists(),
            "predecessor disappeared before transaction completed"
        );
        drop(cache);
        let cache = Cache::open(config(temp.path())).unwrap();
        assert_eq!(cache.cursor(), 102);
        assert_eq!(pack_files(temp.path()).len(), 2);
        for bytes in [b"anchor".as_slice(), b"old-delta", b"new-delta"] {
            assert_eq!(
                contents(&cache, crate::object_id(ObjType::Blob, bytes)),
                bytes
            );
        }

        assert!(!old_path.exists());
    }
}

#[test]
fn long_lived_git_readers_survive_managed_pack_replacement() {
    use std::io::{BufRead, BufReader};
    for midx in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let mut cache = Cache::open(config(temp.path())).unwrap();
        apply(&mut cache, &[b"anchor"], 100);
        apply(&mut cache, &[b"old"], 101);
        if midx {
            checked(git_command(temp.path()).args(["multi-pack-index", "write"])).unwrap();
        }
        let mut child = git_command(temp.path())
            .args(["cat-file", "--batch"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        writeln!(input, "{}", Oid20([0; 20])).unwrap();
        input.flush().unwrap();
        let mut line = String::new();
        output.read_line(&mut line).unwrap();
        assert!(line.ends_with(" missing\n"));
        apply(&mut cache, &[b"new"], 102);
        writeln!(input, "{}", crate::object_id(ObjType::Blob, b"old")).unwrap();
        input.flush().unwrap();
        line.clear();
        output.read_line(&mut line).unwrap();
        assert!(line.ends_with(" blob 3\n"), "{line}");
        let mut bytes = [0; 4];
        output.read_exact(&mut bytes).unwrap();
        assert_eq!(&bytes, b"old\n");
        drop(input);
        assert!(child.wait().unwrap().success());
    }
}

#[test]
fn corrupt_state_and_foreign_server_are_not_silently_reset() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    apply(&mut cache, &[b"anchor"], 100);
    let path = cache.metadata().join("state.bin");
    drop(cache);
    let mut other = config(temp.path());
    other.server = "http://other.test".to_owned();
    assert!(Cache::open(other).is_err());
    let mut bytes = fs::read(&path).unwrap();
    bytes[12] ^= 1;
    fs::write(&path, bytes).unwrap();
    assert!(Cache::open(config(temp.path())).is_err());
    assert_eq!(pack_files(temp.path()).len(), 1);
}

#[test]
fn existing_incremental_midx_is_rebuilt_before_retirement() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    apply(&mut cache, &[b"anchor"], 100);
    apply(&mut cache, &[b"old"], 101);
    checked(git_command(temp.path()).args(["multi-pack-index", "write", "--incremental"])).unwrap();
    apply(&mut cache, &[b"new"], 102);
    checked(git_command(temp.path()).args(["multi-pack-index", "verify"])).unwrap();
    assert_eq!(pack_files(temp.path()).len(), 2);
    assert_eq!(
        contents(&cache, crate::object_id(ObjType::Blob, b"old")),
        b"old"
    );
}

#[test]
fn untracked_reserved_names_are_not_adopted_or_deleted() {
    let temp = tempfile::tempdir().unwrap();
    let mut cache = Cache::open(config(temp.path())).unwrap();
    apply(&mut cache, &[b"anchor"], 100);
    apply(&mut cache, &[b"old"], 101);
    let old = cache.state.delta.unwrap();
    let replacement = pack(&[entry(ObjType::Blob, b"old"), entry(ObjType::Blob, b"new")]);
    let hash = Oid20::from_slice(&replacement[replacement.len() - 20..]).unwrap();
    let path = temp
        .path()
        .join(format!("objects/pack/gvfs-delta-{hash}.pack"));
    fs::write(&path, &replacement).unwrap();
    let (meta, body) = response(&pack(&[entry(ObjType::Blob, b"new")]), 101, 102);
    assert!(
        cache
            .apply_response(200, meta, &mut body.as_slice())
            .is_err()
    );
    assert_eq!(cache.cursor(), 101);
    assert_eq!(cache.state.delta, Some(old));
    assert!(path.exists());
    assert!(cache.path(Role::Delta, old, "pack").exists());
}

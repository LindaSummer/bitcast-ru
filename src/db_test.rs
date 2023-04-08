use bytes::Bytes;
use tempfile::Builder;

use crate::{
    db::Engine,
    error::Errors,
    options::Options,
    utils::rand_kv::{get_test_key, get_test_value},
};

#[test]
fn test_engine_put() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(100));

    assert!(engine.put(get_test_key(101), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));
    assert!(engine.put(get_test_key(101), get_test_value(201)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(201));

    assert!(engine.put(get_test_key(102), Bytes::new()).is_ok());
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    assert_eq!(
        engine.put(Bytes::new(), get_test_value(103)),
        Err(Errors::EmptyKey)
    );

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert!(engine.put(get_test_key(105), get_test_value(105)).is_ok());
    let res = engine.get(get_test_key(105));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(105));
}

#[test]
fn test_engine_get() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(100));

    assert!(engine.put(get_test_key(100), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(100));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));

    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    drop(engine);

    let engine = Engine::open(opts.clone()).expect("failed to open engine");
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(101), get_test_value(101)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(101));
    assert!(engine.put(get_test_key(101), get_test_value(1201)).is_ok());
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));

    assert!(engine.put(get_test_key(102), Bytes::new()).is_ok());
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    assert_eq!(
        engine.put(Bytes::new(), get_test_value(103)),
        Err(Errors::EmptyKey)
    );

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert!(engine.put(get_test_key(105), get_test_value(105)).is_ok());
    let res = engine.get(get_test_key(105));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(105));

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    let res = engine.get(get_test_key(101));
    assert!(res.is_ok());
    assert_eq!(res.unwrap(), get_test_value(1201));
    let res = engine.get(get_test_key(102));
    assert!(res.is_ok());
    assert_eq!(res.unwrap().len(), 0);
}

#[test]
fn test_engine_delete() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));
    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(100), get_test_value(100)).is_ok());
    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(100)));

    assert_eq!(engine.delete(get_test_key(100)), Ok(()));
    assert_eq!(engine.get(get_test_key(100)), Err(Errors::KeyNotFound));

    assert_eq!(engine.delete(Bytes::new()), Err(Errors::EmptyKey));

    assert!(engine.put(get_test_key(100), get_test_value(201)).is_ok());
    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));

    assert!(engine.put(get_test_key(101), get_test_value(202)).is_ok());
    assert_eq!(engine.get(get_test_key(101)), Ok(get_test_value(202)));
    assert_eq!(engine.delete(get_test_key(101)), Ok(()));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));

    assert!(engine.put(get_test_key(102), get_test_value(202)).is_ok());
    assert_eq!(engine.get(get_test_key(102)), Ok(get_test_value(202)));

    for i in 200..=1000000 {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));
    assert_eq!(engine.get(get_test_key(102)), Ok(get_test_value(202)));
    assert_eq!(engine.delete(get_test_key(102)), Ok(()));
    assert_eq!(engine.get(get_test_key(102)), Err(Errors::KeyNotFound));

    drop(engine);

    let engine = Engine::open(opts).expect("failed to open engine");

    assert_eq!(engine.get(get_test_key(100)), Ok(get_test_value(201)));
    assert_eq!(engine.get(get_test_key(101)), Err(Errors::KeyNotFound));
    assert_eq!(engine.get(get_test_key(102)), Err(Errors::KeyNotFound));
}

#[test]
fn test_list_keys_add_and_delete() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.list_keys(), Vec::<Bytes>::default());

    assert_eq!(engine.put("key0".into(), "value0".into()), Ok(()));
    assert_eq!(engine.list_keys(), vec![Bytes::from("key0")]);

    assert_eq!(engine.put("key1".into(), "value1".into()), Ok(()));
    assert_eq!(
        engine.list_keys(),
        vec![Bytes::from("key0"), Bytes::from("key1")]
    );

    assert_eq!(engine.put("key2".into(), "value2".into()), Ok(()));
    assert_eq!(
        engine.list_keys(),
        vec![
            Bytes::from("key0"),
            Bytes::from("key1"),
            Bytes::from("key2")
        ]
    );

    assert_eq!(engine.delete("key1".into()), Ok(()));
    assert_eq!(
        engine.list_keys(),
        vec![Bytes::from("key0"), Bytes::from("key2")]
    );
}

#[test]
fn test_list_keys_large_size() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(engine.list_keys(), Vec::<Bytes>::default());

    const SIZE: usize = 1000000;

    for i in 0..=SIZE {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(
        engine.list_keys(),
        (0..SIZE + 1)
            .map(|x| { get_test_key(x) })
            .collect::<Vec<_>>()
    );

    for i in 0..=SIZE - 10 {
        assert!(engine.delete(get_test_key(i)).is_ok());
    }

    assert_eq!(
        engine.list_keys(),
        (SIZE - 10 + 1..SIZE + 1)
            .map(|x| { get_test_key(x) })
            .collect::<Vec<_>>()
    );
}

#[test]
fn test_fold_keys() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    assert_eq!(
        engine.fold(|_, _| -> bool {
            assert!(false);
            true
        }),
        Ok(())
    );

    const SIZE: usize = 1000000;

    for i in 0..=SIZE {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    let mut key_vec = Vec::new();
    let mut value_vec = Vec::new();
    assert_eq!(
        engine.fold(|key, value| -> bool {
            key_vec.push(key);
            value_vec.push(value);
            false
        }),
        Ok(())
    );

    assert_eq!(key_vec, vec![get_test_key(0)]);
    assert_eq!(value_vec, vec![get_test_value(0)]);

    let mut key_id: usize = 0;
    assert_eq!(
        engine.fold(|key, value| -> bool {
            assert_eq!(key, get_test_key(key_id));
            assert_eq!(value, get_test_value(key_id));
            key_id += 1;
            true
        }),
        Ok(())
    );

    for i in 0..=SIZE - 10 {
        assert!(engine.delete(get_test_key(i)).is_ok());
    }

    let mut key_id: usize = SIZE - 10 + 1;
    assert_eq!(
        engine.fold(|key, value| -> bool {
            assert_eq!(key, get_test_key(key_id));
            assert_eq!(value, get_test_value(key_id));
            key_id += 1;
            true
        }),
        Ok(())
    );
}

#[test]
fn test_close() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    const SIZE: usize = 1000000;

    for i in 0..=SIZE {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.close(), Ok(()));
}

#[test]
fn test_sync() {
    let mut opts = Options::default();
    opts.dir_path = Builder::new()
        .prefix("bitcast-rs")
        .tempdir()
        .unwrap()
        .path()
        .to_path_buf();
    opts.datafile_size = 64 * 1024 * 1024;

    let engine = Engine::open(opts.clone()).expect("failed to open engine");

    const SIZE: usize = 1000000;

    for i in 0..=SIZE {
        assert!(engine.put(get_test_key(i), get_test_value(i)).is_ok());
    }

    assert_eq!(engine.sync(), Ok(()));
}

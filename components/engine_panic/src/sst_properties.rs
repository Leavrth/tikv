// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use engine_traits::SSTPropertiesExt;

use crate::engine::PanicEngine;

impl SSTPropertiesExt for PanicEngine {}

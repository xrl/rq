use byteorder;
use error::{self, Error, ErrorKind};
use header;
use schema;
use serde;
use serde_json;
use std::borrow;
use std::io;
use std::fmt;

pub mod read;
mod util;

pub struct Deserializer<'a, R>
    where R: io::Read + read::Limit
{
    input: R,
    registry: borrow::Cow<'a, schema::SchemaRegistry>,
    schema: borrow::Cow<'a, schema::Schema>,
}

struct DeserializerImpl<'a, R>
    where R: io::Read + 'a
{
    input: &'a mut R,
    registry: &'a schema::SchemaRegistry,
    schema: &'a schema::Schema,
}

struct RecordVisitor<'a, R>
    where R: io::Read + 'a
{
    input: &'a mut R,
    registry: &'a schema::SchemaRegistry,
    fields: schema::RecordFields<'a>,
    field: Option<&'a schema::FieldSchema>,
}

struct FieldNameDeserializer<'a>(&'a str);

enum BlockRemainder {
    Start,
    Count(usize),
    End,
}

struct ArrayVisitor<'a, R>
    where R: io::Read + 'a
{
    input: &'a mut R,
    registry: &'a schema::SchemaRegistry,
    elem_schema: &'a schema::Schema,
    remainder: BlockRemainder,
}

struct MapVisitor<'a, R>
    where R: io::Read + 'a
{
    input: &'a mut R,
    registry: &'a schema::SchemaRegistry,
    value_schema: &'a schema::Schema,
    remainder: BlockRemainder,
}

impl<'a, R> Deserializer<'a, R>
    where R: io::Read + read::Limit
{
    pub fn new(input: R,
               registry: &'a schema::SchemaRegistry,
               schema: &'a schema::Schema)
               -> Deserializer<'a, R> {
        Deserializer::new_cow(input,
                              borrow::Cow::Borrowed(registry),
                              borrow::Cow::Borrowed(schema))
    }

    fn new_cow(input: R,
               registry: borrow::Cow<'a, schema::SchemaRegistry>,
               schema: borrow::Cow<'a, schema::Schema>)
               -> Deserializer<'a, R> {
        Deserializer {
            input: input,
            registry: registry,
            schema: schema,
        }
    }
}

impl<'a, R> Deserializer<'a, read::Blocks<R>>
    where R: io::Read
{
    // TODO: this uses a ridiculous number of buffers... We can cut that down significantly
    pub fn from_container(mut input: R) -> error::Result<Deserializer<'static, read::Blocks<R>>> {
        use serde::de::Deserialize;

        let header = {
            debug!("Parsing container header");
            let direct = read::Direct::new(&mut input, 1);
            let mut header_de =
                Deserializer::new(direct, &schema::EMPTY_REGISTRY, &schema::FILE_HEADER);
            header::Header::deserialize(&mut header_de)?
        };
        debug!("Container header: {:?}", header);

        if &[b'O', b'b', b'j', 1] != &*header.magic {
            Err(ErrorKind::BadFileMagic(header.magic.to_vec()).into())
        } else {
            let codec = read::Codec::parse(header.meta.get("avro.codec").map(AsRef::as_ref))?;
            let schema_data = header.meta
                .get("avro.schema")
                .ok_or(Error::from(ErrorKind::NoSchema))?;

            let schema_json = serde_json::from_slice(&schema_data)?;
            let mut registry = schema::SchemaRegistry::new();

            let root_schema = registry.add_json(&schema_json)?
                .ok_or(Error::from(ErrorKind::NoRootType))?
                .into_resolved(&registry);

            let blocks = read::Blocks::new(input, codec, header.sync.to_vec());
            let registry_cow = borrow::Cow::Owned(registry);
            let schema_cow = borrow::Cow::Owned(root_schema);
            Ok(Deserializer::new_cow(blocks, registry_cow, schema_cow))
        }
    }
}

impl<'a, 'b, R> serde::Deserializer<'a> for &'b mut Deserializer<'a, R>
    where R: io::Read + read::Limit
{
    type Error = error::Error;

    forward_to_deserialize_any! {
        <W: Visitor<'a>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple_struct map
        struct enum identifier ignored_any seq tuple
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'a>
    {
        if !self.input.take_limit()? {
            bail!(error::ErrorKind::EndOfStream)
        }

        unimplemented!()
//        DeserializerImpl::new(&mut self.input, &*self.registry, &*self.schema).deserialize(visitor)
    }
}

impl<'a, R> DeserializerImpl<'a, R>
    where R: io::Read
{
    pub fn new(input: &'a mut R,
               registry: &'a schema::SchemaRegistry,
               schema: &'a schema::Schema)
               -> DeserializerImpl<'a, R> {
        DeserializerImpl {
            input: input,
            registry: registry,
            schema: schema,
        }
    }

    fn deserialize<V>(&mut self, visitor: V) -> Result<V::Value, error::Error>
        where V: serde::de::Visitor<'a>
    {
        use schema::Schema;
        use byteorder::ReadBytesExt;

        match *self.schema {
            Schema::Null => {
                debug!("Deserializing null");
                visitor.visit_unit()
            },
            Schema::Boolean => {
                let v = self.input.read_u8()?;
                debug!("Deserializing boolean {:?}", v);
                // TODO: if v is not in [0, 1], report error
                visitor.visit_bool(v != 0)
            },
            Schema::Int => {
                let v = util::read_int(self.input)?;
                debug!("Deserializing int {:?}", v);
                visitor.visit_i32(v)
            },
            Schema::Long => {
                let v = util::read_long(self.input)?;
                debug!("Deserializing long {:?}", v);
                visitor.visit_i64(v)
            },
            Schema::Float => {
                let v = self.input.read_f32::<byteorder::LittleEndian>()?;
                debug!("Deserializing float {:?}", v);
                visitor.visit_f32(v)
            },
            Schema::Double => {
                let v = self.input.read_f64::<byteorder::LittleEndian>()?;
                debug!("Deserializing double {:?}", v);
                visitor.visit_f64(v)
            },
            Schema::Bytes => {
                let len = util::read_long(self.input)?;

                if len < 0 {
                    Err(ErrorKind::NegativeLength.into())
                } else {
                    let mut result = vec![0; len as usize];
                    self.input.read_exact(&mut result)?;
                    debug!("Deserializing bytes {:?}", result);
                    visitor.visit_byte_buf(result)
                }
            },
            Schema::String => {
                let len = util::read_long(self.input)?;

                if len < 0 {
                    Err(ErrorKind::NegativeLength.into())
                } else {
                    let mut buffer = vec![0; len as usize];
                    self.input.read_exact(&mut buffer)?;
                    let result = String::from_utf8(buffer)?;
                    debug!("Deserializing string {:?}", result);
                    visitor.visit_string(result)
                }
            },
            Schema::Record(ref inner) => {
                debug!("Deserializing record of type {:?}", inner.name());
                unimplemented!()
//                let fields = inner.fields();
//                visitor.visit_map(RecordVisitor::new(self.input, &*self.registry, fields))
            },
            Schema::Enum(ref inner) => {
                debug!("Deserializing enum of type {:?}", inner.name());
                let v = util::read_int(self.input)?;
                visitor.visit_str(inner.symbols()[v as usize].as_str())
            },
            Schema::Array(ref inner) => {
                debug!("Deserializing array");
                unimplemented!()
//                let elem_schema = inner.resolve(&self.registry);
//                visitor.visit_seq(ArrayVisitor::new(self.input, &*self.registry, elem_schema))
            },
            Schema::Map(ref inner) => {
                debug!("Deserializing map");
                unimplemented!()
//                let value_schema = inner.resolve(&self.registry);
//                visitor.visit_map(MapVisitor::new(self.input, &*self.registry, value_schema))
            },
            Schema::Union(ref inner) => {
                debug!("Deserializing union");
//                let variant = util::read_long(self.input)?;
//                let schema = inner[variant as usize].resolve(&self.registry);
                unimplemented!()
//                DeserializerImpl::new(self.input, self.registry, &schema).deserialize(visitor)
            },
            Schema::Fixed(ref inner) => {
                debug!("Deserializing fixed of size {}", inner.size());
                let mut buffer = vec![0; inner.size() as usize];
                self.input.read_exact(&mut buffer)?;
                visitor.visit_byte_buf(buffer)
            },
        }
    }
}


impl<'a, 'b, R> serde::Deserializer<'a> for &'b mut DeserializerImpl<'a, R>
    where R: io::Read
{
    type Error = error::Error;

    forward_to_deserialize_any! {
        <W: Visitor<'a>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple_struct map
        struct enum identifier ignored_any seq tuple
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'a>
    {
        self.deserialize(visitor)
    }
}

impl<'a, R> RecordVisitor<'a, R>
    where R: io::Read
{
    fn new(input: &'a mut R,
           registry: &'a schema::SchemaRegistry,
           fields: schema::RecordFields<'a>)
           -> RecordVisitor<'a, R> {
        RecordVisitor {
            input: input,
            registry: registry,
            fields: fields,
            field: None,
        }
    }
}

impl<'a, R> serde::de::Visitor<'a> for RecordVisitor<'a, R>
    where R: io::Read
{
    type Value=R;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a very special map")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value,M::Error>
        where M: serde::de::MapAccess<'a>
    {
        unimplemented!()
//        if let Some(f) = self.fields.next() {
//            self.field = Some(f);
//            debug!("Deserializing field {:?}", f.name());
//            let k = seed.deserialize(FieldNameDeserializer(f.name()))?;
//            Ok(Some(k))
//        } else {
//            Ok(None)
//        }
    }
}

impl<'a> serde::Deserializer<'a> for FieldNameDeserializer<'a> {
    type Error = error::Error;

    forward_to_deserialize_any! {
        <W: Visitor<'a>>
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct tuple_struct map
        struct enum identifier ignored_any seq tuple
    }

    #[inline]
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where V: serde::de::Visitor<'a>
    {
        visitor.visit_str(self.0)
    }
}

impl BlockRemainder {
    fn next<R: io::Read>(&mut self, reader: &mut R) -> error::Result<bool> {
        match *self {
            BlockRemainder::Start |
            BlockRemainder::Count(0) => {
                let n = util::read_block_size(reader)?;
                if n == 0 {
                    *self = BlockRemainder::End;
                    Ok(false)
                } else {
                    *self = BlockRemainder::Count(n - 1);
                    Ok(true)
                }
            },
            BlockRemainder::Count(n) => {
                if n == 0 {
                    *self = BlockRemainder::End;
                    Ok(false)
                } else {
                    *self = BlockRemainder::Count(n - 1);
                    Ok(true)
                }
            },
            BlockRemainder::End => Ok(false),
        }
    }
}

impl<'a, R> ArrayVisitor<'a, R>
    where R: io::Read
{
    fn new(input: &'a mut R,
           registry: &'a schema::SchemaRegistry,
           elem_schema: &'a schema::Schema)
           -> ArrayVisitor<'a, R> {
        ArrayVisitor {
            input: input,
            registry: registry,
            elem_schema: elem_schema,
            remainder: BlockRemainder::Start,
        }
    }
}

impl<'a, R> serde::de::Visitor<'a> for ArrayVisitor<'a, R>
    where R: io::Read
{
    type Value=R;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a very special map")
    }

    fn visit_seq<M>(self, seq: M) -> Result<Self::Value,M::Error>
        where M: serde::de::SeqAccess<'a>
    {
        if self.remainder.next(self.input)? {
            debug!("Deserializing array element");
            let mut de = DeserializerImpl::new(self.input, self.registry, &self.elem_schema);
            let v = seq.deserialize(&mut de)?;
            Ok(Some(v))
        } else {
            Ok(None)
        }
    }
}

impl<'a, R> MapVisitor<'a, R>
    where R: io::Read
{
    fn new(input: &'a mut R,
           registry: &'a schema::SchemaRegistry,
           value_schema: &'a schema::Schema)
           -> MapVisitor<'a, R> {
        MapVisitor {
            input: input,
            registry: registry,
            value_schema: value_schema,
            remainder: BlockRemainder::Start,
        }
    }
}

impl<'a, R> serde::de::Visitor<'a> for MapVisitor<'a, R>
    where R: io::Read
{
    type Value=R;

    // Format a message stating what data this Visitor expects to receive.
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a very special map")
    }

    fn visit_map<K>(self, seed: K) -> Result<Self::Value, K::Error>
        where K: serde::de::MapAccess<'a>
    {
        if self.remainder.next(&mut self.input)? {
            let schema = schema::Schema::String;
            let mut de = DeserializerImpl::new(self.input, self.registry, &schema);
            let k = seed.deserialize(&mut de)?;
            Ok(Some(k))
        } else {
            Ok(None)
        }
    }
}

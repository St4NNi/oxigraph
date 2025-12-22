#[cfg(feature = "async-tokio")]
use json_event_parser::TokioAsyncWriterJsonSerializer;
use json_event_parser::{JsonEvent, WriterJsonSerializer};
use oxiri::{Iri, IriParseError};
#[cfg(feature = "rdf-12")]
use oxrdf::BaseDirection;
use oxrdf::vocab::xsd;
use oxrdf::{
    GraphName, GraphNameRef, NamedNode, NamedOrBlankNode, NamedOrBlankNodeRef, QuadRef, TermRef,
};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::io;
use std::io::Write;
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::{Arc, Mutex};
#[cfg(feature = "async-tokio")]
use tokio::io::AsyncWrite;

use crate::compaction::JsonLdCompactionConverter;
use crate::context::{JsonLdContext, JsonLdContextProcessor, JsonLdLoadDocumentOptions, JsonLdRemoteDocument, JsonNode};
use crate::profile::JsonLdProcessingMode;
use crate::MAX_CONTEXT_RECURSION;

type LoadDocumentCallback = dyn Fn(
    &str,
    &JsonLdLoadDocumentOptions,
) -> Result<JsonLdRemoteDocument, Box<dyn Error + Send + Sync>>
    + Send
    + Sync
    + UnwindSafe
    + RefUnwindSafe;

/// A [JSON-LD](https://www.w3.org/TR/json-ld/) serializer.
///
/// Returns [Streaming JSON-LD](https://www.w3.org/TR/json-ld11-streaming/).
///
/// It does not implement exactly the [RDF as JSON-LD Algorithm](https://www.w3.org/TR/json-ld-api/#serialize-rdf-as-json-ld-algorithm)
/// to be a streaming serializer but aims at being close to it.
/// Features like `@json` and `@list` generation are not implemented.
///
/// ```
/// use oxrdf::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
/// use oxrdf::vocab::rdf;
/// use oxjsonld::JsonLdSerializer;
///
/// let mut serializer = JsonLdSerializer::new().with_prefix("schema", "http://schema.org/")?.for_writer(Vec::new());
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     rdf::TYPE,
///     NamedNodeRef::new("http://schema.org/Person")?,
///     GraphNameRef::DefaultGraph
/// ))?;
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     NamedNodeRef::new("http://schema.org/name")?,
///     LiteralRef::new_language_tagged_literal_unchecked("Foo Bar", "en"),
///     GraphNameRef::DefaultGraph
/// ))?;
/// assert_eq!(
///     b"{\"@context\":{\"schema\":\"http://schema.org/\"},\"@graph\":[{\"@id\":\"http://example.com#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"http://schema.org/Person\"}],\"http://schema.org/name\":[{\"@language\":\"en\",\"@value\":\"Foo Bar\"}]}]}",
///     serializer.finish()?.as_slice()
/// );
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
#[must_use]
pub struct JsonLdSerializer {
    prefixes: BTreeMap<String, String>,
    base_iri: Option<Iri<String>>,
    /// Context URLs for compaction (multiple allowed)
    context_urls: Vec<String>,
    /// Pre-loaded contexts (URL -> JSON)
    context_cache: HashMap<String, JsonNode>,
    load_document_callback: Option<Arc<LoadDocumentCallback>>,
}

impl Default for JsonLdSerializer {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for JsonLdSerializer {
    fn clone(&self) -> Self {
        Self {
            prefixes: self.prefixes.clone(),
            base_iri: self.base_iri.clone(),
            context_urls: self.context_urls.clone(),
            context_cache: self.context_cache.clone(),
            load_document_callback: self.load_document_callback.clone(),
        }
    }
}

impl JsonLdSerializer {
    /// Builds a new [`JsonLdSerializer`].
    #[inline]
    pub fn new() -> Self {
        Self {
            prefixes: BTreeMap::new(),
            base_iri: None,
            context_urls: Vec::new(),
            context_cache: HashMap::new(),
            load_document_callback: None,
        }
    }

    #[inline]
    pub fn with_prefix(
        mut self,
        prefix_name: impl Into<String>,
        prefix_iri: impl Into<String>,
    ) -> Result<Self, IriParseError> {
        self.prefixes.insert(
            prefix_name.into(),
            Iri::parse(prefix_iri.into())?.into_inner(),
        );
        Ok(self)
    }

    /// Allows to set the base IRI for serialization.
    ///
    /// Corresponds to the [`base` option from the algorithm specification](https://www.w3.org/TR/json-ld-api/#dom-jsonldoptions-base).
    /// ```
    /// use oxrdf::{GraphNameRef, NamedNodeRef, QuadRef};
    /// use oxjsonld::JsonLdSerializer;
    ///
    /// let mut serializer = JsonLdSerializer::new()
    ///     .with_base_iri("http://example.com")?
    ///     .with_prefix("ex", "http://example.com/ns#")?
    ///     .for_writer(Vec::new());
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     NamedNodeRef::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?,
    ///     NamedNodeRef::new("http://example.com/ns#Person")?,
    ///     GraphNameRef::DefaultGraph
    /// ))?;
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     NamedNodeRef::new("http://example.com/ns#parent")?,
    ///     NamedNodeRef::new("http://example.com#other")?,
    ///     GraphNameRef::DefaultGraph
    /// ))?;
    /// assert_eq!(
    ///     b"{\"@context\":{\"@base\":\"http://example.com\",\"ex\":\"http://example.com/ns#\"},\"@graph\":[{\"@id\":\"#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"/ns#Person\"}],\"http://example.com/ns#parent\":[{\"@id\":\"#other\"}]}]}",
    ///     serializer.finish()?.as_slice()
    /// );
    /// # Result::<_,Box<dyn std::error::Error>>::Ok(())
    /// ```
    #[inline]
    pub fn with_base_iri(mut self, base_iri: impl Into<String>) -> Result<Self, IriParseError> {
        self.base_iri = Some(Iri::parse(base_iri.into())?);
        Ok(self)
    }

    /// Add a context URL for compaction. Multiple URLs can be added.
    #[inline]
    pub fn with_context_url(mut self, url: impl Into<String>) -> Self {
        self.context_urls.push(url.into());
        self
    }

    /// Pre-load a context (for offline/bundled contexts).
    /// Also adds the URL to the context list.
    pub fn with_context(mut self, url: impl Into<String>, context: JsonNode) -> Self {
        let url = url.into();
        self.context_cache.insert(url.clone(), context);
        self.context_urls.push(url);
        self
    }

    /// Set callback for loading remote contexts not in cache.
    pub fn with_load_document_callback(
        mut self,
        callback: impl Fn(&str, &JsonLdLoadDocumentOptions)
            -> Result<JsonLdRemoteDocument, Box<dyn Error + Send + Sync>>
            + Send
            + Sync
            + UnwindSafe
            + RefUnwindSafe
            + 'static,
    ) -> Self {
        self.load_document_callback = Some(Arc::new(callback));
        self
    }

    /// Serializes a JSON-LD file to a [`Write`] implementation.
    ///
    /// This writer does unbuffered writes.
    ///
    /// ```
    /// use oxrdf::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
    /// use oxrdf::vocab::rdf;
    /// use oxjsonld::JsonLdSerializer;
    ///
    /// let mut serializer = JsonLdSerializer::new().with_prefix("schema", "http://schema.org/")?.for_writer(Vec::new());
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     rdf::TYPE,
    ///     NamedNodeRef::new("http://schema.org/Person")?,
    ///     GraphNameRef::DefaultGraph
    /// ))?;
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     NamedNodeRef::new("http://schema.org/name")?,
    ///     LiteralRef::new_language_tagged_literal_unchecked("Foo Bar", "en"),
    ///     GraphNameRef::DefaultGraph
    /// ))?;
    /// assert_eq!(
    ///     b"{\"@context\":{\"schema\":\"http://schema.org/\"},\"@graph\":[{\"@id\":\"http://example.com#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"http://schema.org/Person\"}],\"http://schema.org/name\":[{\"@language\":\"en\",\"@value\":\"Foo Bar\"}]}]}",
    ///     serializer.finish()?.as_slice()
    /// );
    /// # Result::<_, Box<dyn std::error::Error>>::Ok(())
    /// ```
    pub fn for_writer<W: Write>(self, writer: W) -> WriterJsonLdSerializer<W> {
        WriterJsonLdSerializer {
            writer: WriterJsonSerializer::new(writer),
            inner: self.inner_writer(),
        }
    }

    /// Serializes a JSON-LD file to a [`AsyncWrite`] implementation.
    ///
    /// This writer does unbuffered writes.
    ///
    /// ```
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use oxrdf::{NamedNodeRef, QuadRef, LiteralRef, GraphNameRef};
    /// use oxrdf::vocab::rdf;
    /// use oxjsonld::JsonLdSerializer;
    ///
    /// let mut serializer = JsonLdSerializer::new().with_prefix("schema", "http://schema.org/")?.for_tokio_async_writer(Vec::new());
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     rdf::TYPE,
    ///     NamedNodeRef::new("http://schema.org/Person")?,
    ///     GraphNameRef::DefaultGraph
    /// )).await?;
    /// serializer.serialize_quad(QuadRef::new(
    ///     NamedNodeRef::new("http://example.com#me")?,
    ///     NamedNodeRef::new("http://schema.org/name")?,
    ///     LiteralRef::new_language_tagged_literal_unchecked("Foo Bar", "en"),
    ///     GraphNameRef::DefaultGraph
    /// )).await?;
    /// assert_eq!(
    ///     b"{\"@context\":{\"schema\":\"http://schema.org/\"},\"@graph\":[{\"@id\":\"http://example.com#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"http://schema.org/Person\"}],\"http://schema.org/name\":[{\"@language\":\"en\",\"@value\":\"Foo Bar\"}]}]}",
    ///     serializer.finish().await?.as_slice()
    /// );
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "async-tokio")]
    pub fn for_tokio_async_writer<W: AsyncWrite + Unpin>(
        self,
        writer: W,
    ) -> TokioAsyncWriterJsonLdSerializer<W> {
        TokioAsyncWriterJsonLdSerializer {
            writer: TokioAsyncWriterJsonSerializer::new(writer),
            inner: self.inner_writer(),
        }
    }

    fn inner_writer(self) -> InnerJsonLdWriter {
        let compaction_enabled = !self.context_urls.is_empty();
        InnerJsonLdWriter {
            started: false,
            current_graph_name: None,
            current_subject: None,
            current_predicate: None,
            emitted_predicates: BTreeSet::new(),
            prefixes: self.prefixes,
            base_iri: self.base_iri,
            context_urls: self.context_urls,
            context_cache: self.context_cache,
            load_document_callback: self.load_document_callback,
            compaction_buffer: if compaction_enabled { Some(Vec::new()) } else { None },
        }
    }
}

/// Serializes a JSON-LD file to a [`Write`] implementation.
///
/// Can be built using [`JsonLdSerializer::for_writer`].
///
/// ```
/// use oxrdf::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
/// use oxrdf::vocab::rdf;
/// use oxjsonld::JsonLdSerializer;
///
/// let mut serializer = JsonLdSerializer::new().with_prefix("schema", "http://schema.org/")?.for_writer(Vec::new());
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     rdf::TYPE,
///     NamedNodeRef::new("http://schema.org/Person")?,
///     GraphNameRef::DefaultGraph
/// ))?;
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     NamedNodeRef::new("http://schema.org/name")?,
///     LiteralRef::new_language_tagged_literal_unchecked("Foo Bar", "en"),
///     GraphNameRef::DefaultGraph
/// ))?;
/// assert_eq!(
///     b"{\"@context\":{\"schema\":\"http://schema.org/\"},\"@graph\":[{\"@id\":\"http://example.com#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"http://schema.org/Person\"}],\"http://schema.org/name\":[{\"@language\":\"en\",\"@value\":\"Foo Bar\"}]}]}",
///     serializer.finish()?.as_slice()
/// );
/// # Result::<_, Box<dyn std::error::Error>>::Ok(())
/// ```
#[must_use]
pub struct WriterJsonLdSerializer<W: Write> {
    writer: WriterJsonSerializer<W>,
    inner: InnerJsonLdWriter,
}

impl<W: Write> WriterJsonLdSerializer<W> {
    /// Serializes an extra quad.
    pub fn serialize_quad<'a>(&mut self, t: impl Into<QuadRef<'a>>) -> io::Result<()> {
        if self.inner.compaction_buffer.is_some() {
            // Compaction mode: buffer owned events
            self.inner.serialize_quad_owned(t)?;
            Ok(())
        } else {
            // Streaming mode: serialize immediately
            let mut buffer = Vec::new();
            self.inner.serialize_quad(t, &mut buffer)?;
            self.flush_buffer(&mut buffer)
        }
    }

    /// Ends the write process and returns the underlying [`Write`].
    pub fn finish(mut self) -> io::Result<W> {
        if let Some(mut buffer) = self.inner.compaction_buffer.take() {
            // Compaction mode: finish buffering, then compact
            self.inner.finish_owned(&mut buffer);
            self.finish_with_compaction(buffer)
        } else {
            // Streaming mode
            let mut buffer = Vec::new();
            self.inner.finish(&mut buffer);
            self.flush_buffer(&mut buffer)?;
            self.writer.finish()
        }
    }

    fn finish_with_compaction(mut self, expanded_events: Vec<JsonEvent<'static>>) -> io::Result<W> {
        let mut compactor = self.inner.build_compactor()?;

        // Start wrapper object with @context
        self.writer.serialize_event(JsonEvent::StartObject)?;
        self.writer.serialize_event(JsonEvent::ObjectKey("@context".into()))?;

        // Emit @context value based on what we have
        let has_prefixes = !self.inner.prefixes.is_empty();
        let url_count = self.inner.context_urls.len();

        if url_count == 1 && !has_prefixes {
            // Single URL, no prefixes: just the string
            self.writer.serialize_event(JsonEvent::String(
                self.inner.context_urls[0].clone().into(),
            ))?;
        } else if url_count == 0 && has_prefixes {
            // Only prefixes: inline object
            self.emit_prefix_context()?;
        } else {
            // Multiple URLs or URLs + prefixes: array
            self.writer.serialize_event(JsonEvent::StartArray)?;
            for url in &self.inner.context_urls {
                self.writer.serialize_event(JsonEvent::String(url.clone().into()))?;
            }
            if has_prefixes {
                self.emit_prefix_context()?;
            }
            self.writer.serialize_event(JsonEvent::EndArray)?;
        }

        // Emit @graph key and compact content
        self.writer.serialize_event(JsonEvent::ObjectKey("@graph".into()))?;

        let mut compacted_events = Vec::new();
        let mut errors = Vec::new();
        for event in expanded_events {
            compactor.convert_event(event, &mut compacted_events, &mut errors);
            for compacted in compacted_events.drain(..) {
                self.writer.serialize_event(compacted)?;
            }
        }

        self.writer.serialize_event(JsonEvent::EndObject)?;
        self.writer.finish()
    }

    fn emit_prefix_context(&mut self) -> io::Result<()> {
        self.writer.serialize_event(JsonEvent::StartObject)?;
        for (prefix, iri) in &self.inner.prefixes {
            let key = if prefix.is_empty() { "@vocab" } else { prefix.as_str() };
            self.writer.serialize_event(JsonEvent::ObjectKey(key.to_owned().into()))?;
            self.writer.serialize_event(JsonEvent::String(iri.clone().into()))?;
        }
        self.writer.serialize_event(JsonEvent::EndObject)
    }

    fn flush_buffer(&mut self, buffer: &mut Vec<JsonEvent<'_>>) -> io::Result<()> {
        for event in buffer.drain(0..) {
            self.writer.serialize_event(event)?;
        }
        Ok(())
    }
}

/// Serializes a JSON-LD file to a [`AsyncWrite`] implementation.
///
/// Can be built using [`JsonLdSerializer::for_tokio_async_writer`].
///
/// ```
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use oxrdf::{NamedNodeRef, QuadRef, LiteralRef, GraphNameRef};
/// use oxrdf::vocab::rdf;
/// use oxjsonld::JsonLdSerializer;
///
/// let mut serializer = JsonLdSerializer::new().with_prefix("schema", "http://schema.org/")?.for_tokio_async_writer(Vec::new());
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     rdf::TYPE,
///     NamedNodeRef::new("http://schema.org/Person")?,
///     GraphNameRef::DefaultGraph
/// )).await?;
/// serializer.serialize_quad(QuadRef::new(
///     NamedNodeRef::new("http://example.com#me")?,
///     NamedNodeRef::new("http://schema.org/name")?,
///     LiteralRef::new_language_tagged_literal_unchecked("Foo Bar", "en"),
///     GraphNameRef::DefaultGraph
/// )).await?;
/// assert_eq!(
///     b"{\"@context\":{\"schema\":\"http://schema.org/\"},\"@graph\":[{\"@id\":\"http://example.com#me\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":[{\"@id\":\"http://schema.org/Person\"}],\"http://schema.org/name\":[{\"@language\":\"en\",\"@value\":\"Foo Bar\"}]}]}",
///     serializer.finish().await?.as_slice()
/// );
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "async-tokio")]
#[must_use]
pub struct TokioAsyncWriterJsonLdSerializer<W: AsyncWrite + Unpin> {
    writer: TokioAsyncWriterJsonSerializer<W>,
    inner: InnerJsonLdWriter,
}

#[cfg(feature = "async-tokio")]
impl<W: AsyncWrite + Unpin> TokioAsyncWriterJsonLdSerializer<W> {
    /// Serializes an extra quad.
    pub async fn serialize_quad<'a>(&mut self, t: impl Into<QuadRef<'a>>) -> io::Result<()> {
        let mut buffer = Vec::new();
        self.inner.serialize_quad(t, &mut buffer)?;
        self.flush_buffer(&mut buffer).await
    }

    /// Ends the write process and returns the underlying [`Write`].
    pub async fn finish(mut self) -> io::Result<W> {
        let mut buffer = Vec::new();
        self.inner.finish(&mut buffer);
        self.flush_buffer(&mut buffer).await?;
        self.writer.finish()
    }

    async fn flush_buffer(&mut self, buffer: &mut Vec<JsonEvent<'_>>) -> io::Result<()> {
        for event in buffer.drain(0..) {
            self.writer.serialize_event(event).await?;
        }
        Ok(())
    }
}

pub struct InnerJsonLdWriter {
    started: bool,
    current_graph_name: Option<GraphName>,
    current_subject: Option<NamedOrBlankNode>,
    current_predicate: Option<NamedNode>,
    emitted_predicates: BTreeSet<String>,
    prefixes: BTreeMap<String, String>,
    base_iri: Option<Iri<String>>,
    context_urls: Vec<String>,
    context_cache: HashMap<String, JsonNode>,
    load_document_callback: Option<Arc<LoadDocumentCallback>>,
    /// Buffer for compaction mode - stores all events until finish
    compaction_buffer: Option<Vec<JsonEvent<'static>>>,
}

impl InnerJsonLdWriter {
    fn serialize_quad<'a>(
        &mut self,
        quad: impl Into<QuadRef<'a>>,
        output: &mut Vec<JsonEvent<'a>>,
    ) -> io::Result<()> {
        if !self.started {
            self.serialize_start(output);
            self.started = true;
        }

        let quad = quad.into();
        if self
            .current_graph_name
            .as_ref()
            .is_some_and(|graph_name| graph_name.as_ref() != quad.graph_name)
        {
            output.push(JsonEvent::EndArray);
            output.push(JsonEvent::EndObject);
            if self
                .current_graph_name
                .as_ref()
                .is_some_and(|g| !g.is_default_graph())
            {
                output.push(JsonEvent::EndArray);
                output.push(JsonEvent::EndObject);
            }
            self.current_graph_name = None;
            self.current_subject = None;
            self.current_predicate = None;
            self.emitted_predicates.clear();
        } else if self
            .current_subject
            .as_ref()
            .is_some_and(|subject| subject.as_ref() != quad.subject)
            || self
                .current_predicate
                .as_ref()
                .is_some_and(|predicate| predicate.as_ref() != quad.predicate)
                && self.emitted_predicates.contains(quad.predicate.as_str())
        {
            output.push(JsonEvent::EndArray);
            output.push(JsonEvent::EndObject);
            self.current_subject = None;
            self.emitted_predicates.clear();
            self.current_predicate = None;
        } else if self
            .current_predicate
            .as_ref()
            .is_some_and(|predicate| predicate.as_ref() != quad.predicate)
        {
            output.push(JsonEvent::EndArray);
            if let Some(current_predicate) = self.current_predicate.take() {
                self.emitted_predicates
                    .insert(current_predicate.into_string());
            }
        }

        if self.current_graph_name.is_none() {
            if !quad.graph_name.is_default_graph() {
                // We open a new graph name
                output.push(JsonEvent::StartObject);
                output.push(JsonEvent::ObjectKey("@id".into()));
                output.push(JsonEvent::String(self.id_value(match quad.graph_name {
                    GraphNameRef::NamedNode(iri) => iri.into(),
                    GraphNameRef::BlankNode(bnode) => bnode.into(),
                    GraphNameRef::DefaultGraph => unreachable!(),
                })));
                output.push(JsonEvent::ObjectKey("@graph".into()));
                output.push(JsonEvent::StartArray);
            }
            self.current_graph_name = Some(quad.graph_name.into_owned());
        }

        // We open a new subject block if useful (ie. new subject or already used predicate)
        if self.current_subject.is_none() {
            output.push(JsonEvent::StartObject);
            output.push(JsonEvent::ObjectKey("@id".into()));
            #[allow(
                unreachable_patterns,
                clippy::match_wildcard_for_single_variants,
                clippy::allow_attributes
            )]
            output.push(JsonEvent::String(self.id_value(match quad.subject {
                NamedOrBlankNodeRef::NamedNode(iri) => iri.into(),
                NamedOrBlankNodeRef::BlankNode(bnode) => bnode.into(),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "JSON-LD does not support RDF 1.2 yet",
                    ));
                }
            })));
            self.current_subject = Some(quad.subject.into_owned());
        }

        // We open a predicate key
        if self.current_predicate.is_none() {
            output.push(JsonEvent::ObjectKey(
                // TODO: use @type
                quad.predicate.as_str().into(), // TODO: prefixes including @vocab
            ));
            output.push(JsonEvent::StartArray);
            self.current_predicate = Some(quad.predicate.into_owned());
        }

        self.serialize_term(quad.object, output)
    }

    fn serialize_start(&self, output: &mut Vec<JsonEvent<'_>>) {
        if self.base_iri.is_some() || !self.prefixes.is_empty() {
            output.push(JsonEvent::StartObject);
            output.push(JsonEvent::ObjectKey("@context".into()));
            output.push(JsonEvent::StartObject);
            if let Some(base_iri) = &self.base_iri {
                output.push(JsonEvent::ObjectKey("@base".into()));
                output.push(JsonEvent::String(base_iri.to_string().into()));
            }
            for (prefix_name, prefix_iri) in &self.prefixes {
                output.push(JsonEvent::ObjectKey(if prefix_name.is_empty() {
                    "@vocab".into()
                } else {
                    prefix_name.clone().into()
                }));
                output.push(JsonEvent::String(prefix_iri.clone().into()));
            }
            output.push(JsonEvent::EndObject);
            output.push(JsonEvent::ObjectKey("@graph".into()));
        }
        output.push(JsonEvent::StartArray);
    }

    fn serialize_term<'a>(
        &self,
        term: TermRef<'a>,
        output: &mut Vec<JsonEvent<'a>>,
    ) -> io::Result<()> {
        output.push(JsonEvent::StartObject);
        #[allow(
            unreachable_patterns,
            clippy::match_wildcard_for_single_variants,
            clippy::allow_attributes
        )]
        match term {
            TermRef::NamedNode(iri) => {
                output.push(JsonEvent::ObjectKey("@id".into()));
                output.push(JsonEvent::String(self.id_value(iri.into())));
            }
            TermRef::BlankNode(bnode) => {
                output.push(JsonEvent::ObjectKey("@id".into()));
                output.push(JsonEvent::String(self.id_value(bnode.into())));
            }
            TermRef::Literal(literal) => {
                if let Some(language) = literal.language() {
                    output.push(JsonEvent::ObjectKey("@language".into()));
                    output.push(JsonEvent::String(language.into()));
                    #[cfg(feature = "rdf-12")]
                    if let Some(direction) = literal.direction() {
                        output.push(JsonEvent::ObjectKey("@direction".into()));
                        output.push(JsonEvent::String(
                            match direction {
                                BaseDirection::Ltr => "ltr",
                                BaseDirection::Rtl => "rtl",
                            }
                            .into(),
                        ));
                    }
                } else if literal.datatype() != xsd::STRING {
                    output.push(JsonEvent::ObjectKey("@type".into()));
                    output.push(JsonEvent::String(Self::type_value(
                        literal.datatype().into(),
                    )));
                }
                output.push(JsonEvent::ObjectKey("@value".into()));
                output.push(JsonEvent::String(literal.value().into()));
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "JSON-LD does not support RDF 1.2 yet",
                ));
            }
        }
        output.push(JsonEvent::EndObject);
        Ok(())
    }

    fn id_value<'a>(&self, id: NamedOrBlankNodeRef<'a>) -> Cow<'a, str> {
        match id {
            NamedOrBlankNodeRef::NamedNode(iri) => {
                if let Some(base_iri) = &self.base_iri {
                    if let Ok(relative) = base_iri.relativize(&Iri::parse_unchecked(iri.as_str())) {
                        let relative = relative.into_inner();
                        // We check the relative IRI is not considered as absolute by IRI expansion
                        if !relative.split_once(':').is_some_and(|(prefix, suffix)| {
                            prefix == "_" || suffix.starts_with("//")
                        }) {
                            return relative.into();
                        }
                    }
                }
                iri.as_str().into()
            }
            NamedOrBlankNodeRef::BlankNode(bnode) => bnode.to_string().into(),
        }
    }

    fn type_value(id: NamedOrBlankNodeRef<'_>) -> Cow<'_, str> {
        match id {
            NamedOrBlankNodeRef::NamedNode(iri) => iri.as_str().into(),
            NamedOrBlankNodeRef::BlankNode(bnode) => bnode.to_string().into(),
        }
    }

    fn finish(&mut self, output: &mut Vec<JsonEvent<'static>>) {
        if !self.started {
            self.serialize_start(output);
        }
        if self.current_predicate.is_some() {
            output.push(JsonEvent::EndArray)
        }
        if self.current_subject.is_some() {
            output.push(JsonEvent::EndObject)
        }
        if self
            .current_graph_name
            .as_ref()
            .is_some_and(|g| !g.is_default_graph())
        {
            output.push(JsonEvent::EndArray);
            output.push(JsonEvent::EndObject)
        }
        output.push(JsonEvent::EndArray);
        if self.base_iri.is_some() || !self.prefixes.is_empty() {
            output.push(JsonEvent::EndObject);
        }
    }

    /// Serialize quad with owned events (for compaction mode)
    fn serialize_quad_owned<'a>(&mut self, quad: impl Into<QuadRef<'a>>) -> io::Result<()> {
        let quad = quad.into();

        // Collect events into a local buffer first, then extend compaction_buffer
        let mut events: Vec<JsonEvent<'static>> = Vec::new();

        if !self.started {
            events.push(JsonEvent::StartArray);
            self.started = true;
        }

        // Handle graph/subject/predicate changes
        if self.current_graph_name.as_ref().is_some_and(|g| g.as_ref() != quad.graph_name) {
            events.push(JsonEvent::EndArray);
            events.push(JsonEvent::EndObject);
            if self.current_graph_name.as_ref().is_some_and(|g| !g.is_default_graph()) {
                events.push(JsonEvent::EndArray);
                events.push(JsonEvent::EndObject);
            }
            self.current_graph_name = None;
            self.current_subject = None;
            self.current_predicate = None;
            self.emitted_predicates.clear();
        } else if self.current_subject.as_ref().is_some_and(|s| s.as_ref() != quad.subject)
            || self.current_predicate.as_ref().is_some_and(|p| p.as_ref() != quad.predicate)
                && self.emitted_predicates.contains(quad.predicate.as_str())
        {
            events.push(JsonEvent::EndArray);
            events.push(JsonEvent::EndObject);
            self.current_subject = None;
            self.emitted_predicates.clear();
            self.current_predicate = None;
        } else if self.current_predicate.as_ref().is_some_and(|p| p.as_ref() != quad.predicate) {
            events.push(JsonEvent::EndArray);
            if let Some(p) = self.current_predicate.take() {
                self.emitted_predicates.insert(p.into_string());
            }
        }

        if self.current_graph_name.is_none() {
            if !quad.graph_name.is_default_graph() {
                events.push(JsonEvent::StartObject);
                events.push(JsonEvent::ObjectKey("@id".to_owned().into()));
                let id = match quad.graph_name {
                    GraphNameRef::NamedNode(n) => self.id_value(n.into()).into_owned(),
                    GraphNameRef::BlankNode(b) => self.id_value(b.into()).into_owned(),
                    GraphNameRef::DefaultGraph => unreachable!(),
                };
                events.push(JsonEvent::String(id.into()));
                events.push(JsonEvent::ObjectKey("@graph".to_owned().into()));
                events.push(JsonEvent::StartArray);
            }
            self.current_graph_name = Some(quad.graph_name.into_owned());
        }

        if self.current_subject.is_none() {
            events.push(JsonEvent::StartObject);
            events.push(JsonEvent::ObjectKey("@id".to_owned().into()));
            #[allow(unreachable_patterns, clippy::match_wildcard_for_single_variants)]
            let id = match quad.subject {
                NamedOrBlankNodeRef::NamedNode(n) => self.id_value(n.into()).into_owned(),
                NamedOrBlankNodeRef::BlankNode(b) => self.id_value(b.into()).into_owned(),
                _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "JSON-LD does not support RDF 1.2 yet")),
            };
            events.push(JsonEvent::String(id.into()));
            self.current_subject = Some(quad.subject.into_owned());
        }

        if self.current_predicate.is_none() {
            events.push(JsonEvent::ObjectKey(quad.predicate.as_str().to_owned().into()));
            events.push(JsonEvent::StartArray);
            self.current_predicate = Some(quad.predicate.into_owned());
        }

        Self::serialize_term_owned(&self.base_iri, quad.object, &mut events)?;

        self.compaction_buffer.as_mut().expect("compaction buffer").extend(events);
        Ok(())
    }

    fn serialize_term_owned(base_iri: &Option<Iri<String>>, term: TermRef<'_>, output: &mut Vec<JsonEvent<'static>>) -> io::Result<()> {
        output.push(JsonEvent::StartObject);
        #[allow(unreachable_patterns, clippy::match_wildcard_for_single_variants)]
        match term {
            TermRef::NamedNode(iri) => {
                output.push(JsonEvent::ObjectKey("@id".to_owned().into()));
                output.push(JsonEvent::String(Self::id_value_static(base_iri, iri.into()).into()));
            }
            TermRef::BlankNode(bnode) => {
                output.push(JsonEvent::ObjectKey("@id".to_owned().into()));
                output.push(JsonEvent::String(bnode.to_string().into()));
            }
            TermRef::Literal(literal) => {
                if let Some(language) = literal.language() {
                    output.push(JsonEvent::ObjectKey("@language".to_owned().into()));
                    output.push(JsonEvent::String(language.to_owned().into()));
                    #[cfg(feature = "rdf-12")]
                    if let Some(direction) = literal.direction() {
                        output.push(JsonEvent::ObjectKey("@direction".to_owned().into()));
                        output.push(JsonEvent::String(match direction {
                            BaseDirection::Ltr => "ltr",
                            BaseDirection::Rtl => "rtl",
                        }.to_owned().into()));
                    }
                } else if literal.datatype() != xsd::STRING {
                    output.push(JsonEvent::ObjectKey("@type".to_owned().into()));
                    output.push(JsonEvent::String(literal.datatype().as_str().to_owned().into()));
                }
                output.push(JsonEvent::ObjectKey("@value".to_owned().into()));
                output.push(JsonEvent::String(literal.value().to_owned().into()));
            }
            _ => return Err(io::Error::new(io::ErrorKind::InvalidInput, "JSON-LD does not support RDF 1.2 yet")),
        }
        output.push(JsonEvent::EndObject);
        Ok(())
    }

    fn id_value_static(base_iri: &Option<Iri<String>>, id: NamedOrBlankNodeRef<'_>) -> String {
        match id {
            NamedOrBlankNodeRef::NamedNode(iri) => {
                if let Some(base) = base_iri {
                    if let Ok(relative) = base.relativize(&Iri::parse_unchecked(iri.as_str())) {
                        let relative = relative.into_inner();
                        if !relative.split_once(':').is_some_and(|(prefix, suffix)| {
                            prefix == "_" || suffix.starts_with("//")
                        }) {
                            return relative;
                        }
                    }
                }
                iri.as_str().to_owned()
            }
            NamedOrBlankNodeRef::BlankNode(bnode) => bnode.to_string(),
        }
    }

    fn finish_owned(&mut self, output: &mut Vec<JsonEvent<'static>>) {
        if !self.started {
            output.push(JsonEvent::StartArray);
        }
        if self.current_predicate.is_some() {
            output.push(JsonEvent::EndArray);
        }
        if self.current_subject.is_some() {
            output.push(JsonEvent::EndObject);
        }
        if self.current_graph_name.as_ref().is_some_and(|g| !g.is_default_graph()) {
            output.push(JsonEvent::EndArray);
            output.push(JsonEvent::EndObject);
        }
        output.push(JsonEvent::EndArray);
    }

    fn build_compactor(&self) -> io::Result<JsonLdCompactionConverter> {
        // Pre-populate cache with local contexts
        let mut cache = HashMap::new();
        for (url, node) in &self.context_cache {
            let url_iri = Iri::parse(url.clone()).ok();
            cache.insert(url.clone(), (url_iri, node.clone()));
        }

        let context_processor = JsonLdContextProcessor {
            processing_mode: JsonLdProcessingMode::JsonLd1_0,
            lenient: false,
            max_context_recursion: MAX_CONTEXT_RECURSION,
            remote_context_cache: Arc::new(Mutex::new(cache)),
            load_document_callback: self.load_document_callback.clone(),
        };

        // Build combined context from all URLs
        let mut combined_context = JsonLdContext::new_empty(None);
        for url in &self.context_urls {
            let mut errors = Vec::new();
            combined_context = context_processor.process_context(
                &combined_context,
                JsonNode::String(url.clone()),
                None,
                &mut Vec::new(),
                false,
                true,
                true,
                &mut errors,
            );
            if !errors.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to load context {}: {:?}", url, errors),
                ));
            }
        }

        // Add inline prefixes to context
        for (prefix, iri) in &self.prefixes {
            combined_context.term_definitions.insert(
                prefix.clone(),
                crate::context::JsonLdTermDefinition {
                    iri_mapping: Some(Some(iri.clone())),
                    prefix_flag: true,
                    protected: false,
                    reverse_property: false,
                    base_url: None,
                    context: None,
                    container_mapping: &[],
                    direction_mapping: None,
                    index_mapping: None,
                    language_mapping: None,
                    nest_value: None,
                    type_mapping: None,
                },
            );
        }

        Ok(JsonLdCompactionConverter::new(combined_context, self.base_iri.clone()))
    }
}

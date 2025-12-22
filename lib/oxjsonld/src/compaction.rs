use crate::context::{JsonLdContext, JsonNode};
use crate::error::JsonLdSyntaxError;
use json_event_parser::JsonEvent;
use oxiri::Iri;
use std::borrow::Cow;
use std::collections::HashMap;

enum JsonLdCompactionState {
    /// Initial state, at document root level
    Root,
    /// Inside the @graph array at root level
    RootGraph,
    /// Inside an object, processing properties
    Object {
        /// Buffer of properties: (key, value_events)
        buffer: Vec<(String, Vec<JsonEvent<'static>>)>,
        /// Current nesting depth for buffering
        depth: usize,
        /// Current key being processed
        current_key: Option<String>,
    },
    /// Inside an array
    Array,
    /// Skip state for recovery
    Skip { depth: usize },
}

/// Applies the JSON-LD Compaction Algorithm
/// Takes expanded JSON-LD (as JsonEvent stream) and produces compacted JSON-LD
pub struct JsonLdCompactionConverter {
    /// Active context for compaction
    context: JsonLdContext,
    /// Base IRI for relative IRI compaction
    base_iri: Option<Iri<String>>,
    /// Whether compaction is complete
    is_end: bool,
    /// State machine stack
    state: Vec<JsonLdCompactionState>,
}

impl JsonLdCompactionConverter {
    /// Create a new compaction converter with the given context
    pub fn new(mut context: JsonLdContext, base_iri: Option<Iri<String>>) -> Self {
        context.build_iri_lookup();
        Self {
            context,
            base_iri,
            is_end: false,
            state: vec![JsonLdCompactionState::Root],
        }
    }

    /// Create with a context parsed from JSON
    pub fn with_context_json(
        _context_json: &[u8],
        _base_iri: Option<Iri<String>>,
    ) -> Result<Self, JsonLdSyntaxError> {
        // Parse the context JSON and process it
        // Use JsonLdContextProcessor to process the context
        todo!()
    }

    pub fn is_end(&self) -> bool {
        self.is_end
    }

    /// Get a reference to the active context
    pub fn context(&self) -> &JsonLdContext {
        &self.context
    }

    /// Process a single JSON event and output compacted events
    pub fn convert_event(
        &mut self,
        event: JsonEvent<'_>,
        results: &mut Vec<JsonEvent<'static>>,
        errors: &mut Vec<JsonLdSyntaxError>,
    ) {
        if event == JsonEvent::Eof {
            self.is_end = true;
            return;
        }

        let Some(state) = self.state.pop() else {
            return;
        };

        match state {
            JsonLdCompactionState::Root => {
                self.handle_root(event, results, errors);
            }
            JsonLdCompactionState::RootGraph => {
                self.handle_root_graph(event, results, errors);
            }
            JsonLdCompactionState::Object { buffer, depth, current_key } => {
                self.handle_object(event, buffer, depth, current_key, results, errors);
            }
            JsonLdCompactionState::Array => {
                self.handle_array(event, results, errors);
            }
            JsonLdCompactionState::Skip { depth } => {
                self.handle_skip(event, depth, results);
            }
        }
    }

    fn handle_root(&mut self, event: JsonEvent<'_>, results: &mut Vec<JsonEvent<'static>>, _errors: &mut Vec<JsonLdSyntaxError>) {
        match event {
            JsonEvent::StartObject => {
                results.push(JsonEvent::StartObject);
                self.state.push(JsonLdCompactionState::Object {
                    buffer: Vec::new(),
                    depth: 1,
                    current_key: None,
                });
            }
            JsonEvent::StartArray => {
                results.push(JsonEvent::StartArray);
                self.state.push(JsonLdCompactionState::Array);
            }
            _ => {
                // Pass through other events
                results.push(to_owned_event(event));
            }
        }
    }

    fn handle_root_graph(&mut self, event: JsonEvent<'_>, results: &mut Vec<JsonEvent<'static>>, _errors: &mut Vec<JsonLdSyntaxError>) {
        match event {
            JsonEvent::EndArray => {
                results.push(JsonEvent::EndArray);
                // Stay in root after @graph ends
            }
            JsonEvent::StartObject => {
                results.push(JsonEvent::StartObject);
                self.state.push(JsonLdCompactionState::RootGraph);
                self.state.push(JsonLdCompactionState::Object {
                    buffer: Vec::new(),
                    depth: 1,
                    current_key: None,
                });
            }
            _ => {
                self.state.push(JsonLdCompactionState::RootGraph);
                results.push(to_owned_event(event));
            }
        }
    }

    fn handle_object(
        &mut self,
        event: JsonEvent<'_>,
        mut buffer: Vec<(String, Vec<JsonEvent<'static>>)>,
        mut depth: usize,
        mut current_key: Option<String>,
        results: &mut Vec<JsonEvent<'static>>,
        errors: &mut Vec<JsonLdSyntaxError>,
    ) {
        match event {
            JsonEvent::ObjectKey(key) => {
                if depth == 1 {
                    // Top-level key in this object
                    buffer.push((key.to_string(), Vec::new()));
                    current_key = Some(key.to_string());
                } else {
                    // Nested key, add to current value buffer
                    if let Some((_, events)) = buffer.last_mut() {
                        events.push(JsonEvent::ObjectKey(Cow::Owned(key.to_string())));
                    }
                }
                self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
            }
            JsonEvent::StartObject => {
                if let Some((_, events)) = buffer.last_mut() {
                    events.push(JsonEvent::StartObject);
                }
                depth += 1;
                self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
            }
            JsonEvent::EndObject => {
                depth -= 1;
                if depth == 0 {
                    // End of this object - emit compacted properties
                    self.emit_compacted_object(buffer, results, errors);
                    results.push(JsonEvent::EndObject);
                } else {
                    if let Some((_, events)) = buffer.last_mut() {
                        events.push(JsonEvent::EndObject);
                    }
                    self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
                }
            }
            JsonEvent::StartArray => {
                if let Some((_, events)) = buffer.last_mut() {
                    events.push(JsonEvent::StartArray);
                }
                depth += 1;
                self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
            }
            JsonEvent::EndArray => {
                depth -= 1;
                if let Some((_, events)) = buffer.last_mut() {
                    events.push(JsonEvent::EndArray);
                }
                self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
            }
            _ => {
                // String, Number, Boolean, Null
                if let Some((_, events)) = buffer.last_mut() {
                    events.push(to_owned_event(event));
                }
                self.state.push(JsonLdCompactionState::Object { buffer, depth, current_key });
            }
        }
    }

    fn handle_array(&mut self, event: JsonEvent<'_>, results: &mut Vec<JsonEvent<'static>>, _errors: &mut Vec<JsonLdSyntaxError>) {
        match event {
            JsonEvent::EndArray => {
                results.push(JsonEvent::EndArray);
            }
            JsonEvent::StartObject => {
                results.push(JsonEvent::StartObject);
                self.state.push(JsonLdCompactionState::Array);
                self.state.push(JsonLdCompactionState::Object {
                    buffer: Vec::new(),
                    depth: 1,
                    current_key: None,
                });
            }
            JsonEvent::StartArray => {
                results.push(JsonEvent::StartArray);
                self.state.push(JsonLdCompactionState::Array);
                self.state.push(JsonLdCompactionState::Array);
            }
            _ => {
                self.state.push(JsonLdCompactionState::Array);
                results.push(to_owned_event(event));
            }
        }
    }

    fn handle_skip(&mut self, event: JsonEvent<'_>, mut depth: usize, _results: &mut Vec<JsonEvent<'static>>) {
        match event {
            JsonEvent::StartObject | JsonEvent::StartArray => {
                depth += 1;
                self.state.push(JsonLdCompactionState::Skip { depth });
            }
            JsonEvent::EndObject | JsonEvent::EndArray => {
                depth -= 1;
                if depth > 0 {
                    self.state.push(JsonLdCompactionState::Skip { depth });
                }
            }
            _ => {
                self.state.push(JsonLdCompactionState::Skip { depth });
            }
        }
    }

    fn emit_compacted_object(
        &mut self,
        buffer: Vec<(String, Vec<JsonEvent<'static>>)>,
        results: &mut Vec<JsonEvent<'static>>,
        errors: &mut Vec<JsonLdSyntaxError>,
    ) {
        for (key, value_events) in buffer {
            // Compact the property IRI
            let compacted_key = if key.starts_with('@') {
                // Keywords stay as-is
                key.clone()
            } else {
                self.compact_iri(&key, true, false).unwrap_or(key.clone())
            };

            // Check for @graph at root level
            if compacted_key == "@graph" {
                results.push(JsonEvent::ObjectKey(Cow::Owned(compacted_key)));
                // Process @graph value
                self.state.push(JsonLdCompactionState::RootGraph);
                for event in value_events {
                    self.convert_event(event, results, errors);
                }
                continue;
            }

            results.push(JsonEvent::ObjectKey(Cow::Owned(compacted_key.clone())));

            // Emit value events with compaction
            self.emit_compacted_value(&compacted_key, value_events, results, errors);
        }
    }

    fn emit_compacted_value(
        &self,
        property: &str,
        events: Vec<JsonEvent<'static>>,
        results: &mut Vec<JsonEvent<'static>>,
        _errors: &mut Vec<JsonLdSyntaxError>,
    ) {
        const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";

        // Check if property is @type or rdf:type (which means values are type IRIs)
        let is_type_property = property == "@type" || property == RDF_TYPE;
        // Check if property is @id (which means value is an IRI)
        let is_id_property = property == "@id";

        // Track nesting to know when we're inside @id or @type values within objects
        let mut in_type_context = false;
        let mut depth = 0;

        for event in events {
            match &event {
                JsonEvent::ObjectKey(k) => {
                    if depth == 1 {
                        // Inside a value object, track if we're in @id or @type context
                        in_type_context = k == "@id" || k == "@type";
                    }
                    results.push(JsonEvent::ObjectKey(Cow::Owned(k.to_string())));
                }
                JsonEvent::String(s) => {
                    // Compact IRI if:
                    // 1. We're in a @id/@type context within a value object, OR
                    // 2. The property itself is @type/rdf:type (so values are type IRIs), OR
                    // 3. The property itself is @id
                    if in_type_context || is_type_property || is_id_property {
                        let compacted = self.compact_iri(s, true, false)
                            .unwrap_or_else(|| s.to_string());
                        results.push(JsonEvent::String(Cow::Owned(compacted)));
                    } else {
                        results.push(event);
                    }
                }
                JsonEvent::StartObject => {
                    depth += 1;
                    results.push(event);
                }
                JsonEvent::EndObject => {
                    depth -= 1;
                    results.push(event);
                }
                JsonEvent::StartArray => {
                    depth += 1;
                    results.push(event);
                }
                JsonEvent::EndArray => {
                    depth -= 1;
                    results.push(event);
                }
                _ => {
                    results.push(event);
                }
            }
        }
    }

    /// Compacts an IRI according to the active context
    ///
    /// Parameters:
    /// - iri: The IRI to compact
    /// - vocab: If true, use @vocab for relative compaction
    /// - reverse: If true, this is a reverse property
    ///
    /// Returns the compacted IRI string, or None if it can't be compacted
    pub fn compact_iri(&self, iri: &str, vocab: bool, _reverse: bool) -> Option<String> {
        // 1) If iri is a keyword, return as-is
        if iri.starts_with('@') {
            return Some(iri.to_string());
        }

        // 2) If vocab is true, try term lookup first
        if vocab {
            if let Some(term) = self.context.term_for_iri(iri) {
                return Some(term.to_string());
            }
        }

        // 3) Try to create a compact IRI using a prefix
        if let Some((prefix, suffix)) = self.context.prefix_for_iri(iri) {
            // Validate the compact IRI won't be confused
            let compact = format!("{}:{}", prefix, suffix);
            // Make sure suffix doesn't start with // (would look like absolute IRI)
            if !suffix.starts_with("//") {
                return Some(compact);
            }
        }

        // 4) If vocab is true and @vocab is set, try vocab-relative
        if vocab {
            if let Some(vocab_mapping) = &self.context.vocabulary_mapping {
                if let Some(suffix) = iri.strip_prefix(vocab_mapping.as_str()) {
                    // Make sure it doesn't look like a compact IRI or absolute IRI
                    if !suffix.contains(':') && !suffix.starts_with("//") {
                        return Some(suffix.to_string());
                    }
                }
            }
        }

        // 5) If base IRI is set, try relative IRI compaction
        if let Some(base_iri) = &self.base_iri {
            if let Ok(base) = Iri::parse(base_iri.as_str()) {
                if let Ok(iri_parsed) = Iri::parse(iri) {
                    if let Ok(relative) = base.relativize(&iri_parsed) {
                        let relative_str = relative.into_inner();
                        // Check the relative IRI won't be confused with compact IRI
                        if !relative_str.split_once(':').is_some_and(|(prefix, suffix)| {
                            prefix == "_" || suffix.starts_with("//")
                        }) {
                            return Some(relative_str);
                        }
                    }
                }
            }
        }

        // 6) Return the full IRI
        Some(iri.to_string())
    }

    /// Compacts a value according to the active context and active property
    ///
    /// This handles:
    /// - Simple node references: {"@id": "..."} -> compacted IRI or string
    /// - Value objects: {"@value": ..., "@type": ..., "@language": ...}
    /// - Lists: {"@list": [...]}
    ///
    /// Parameters:
    /// - active_property: The property this value belongs to (for type/language coercion)
    /// - value: The expanded value to compact
    ///
    /// Returns the compacted value
    pub fn compact_value(&self, active_property: Option<&str>, value: &JsonNode) -> JsonNode {
        let JsonNode::Object(obj) = value else {
            return value.clone();
        };

        // Get term definition for active property (if any)
        let term_def = active_property.and_then(|p| self.context.term_definitions.get(p));

        // Handle node reference: {"@id": iri}
        if obj.len() == 1 {
            if let Some(JsonNode::String(id)) = obj.get("@id") {
                // Check if term has @type: @id coercion
                if let Some(def) = term_def {
                    if def.type_mapping.as_deref() == Some("@id") {
                        // Compact to just the IRI string
                        return JsonNode::String(
                            self.compact_iri(id, true, false).unwrap_or_else(|| id.clone())
                        );
                    }
                }
                // Return as compacted node reference
                let mut result = HashMap::new();
                result.insert(
                    "@id".to_string(),
                    JsonNode::String(self.compact_iri(id, false, false).unwrap_or_else(|| id.clone()))
                );
                return JsonNode::Object(result);
            }
        }

        // Handle value object: {"@value": ..., "@type"?: ..., "@language"?: ...}
        if let Some(val) = obj.get("@value") {
            let has_type = obj.contains_key("@type");
            let has_language = obj.contains_key("@language");

            // Try to compact to simple value
            if !has_type && !has_language {
                // Plain value without type or language
                return val.clone();
            }

            // Check if term's type coercion matches
            if let Some(def) = term_def {
                if let Some(type_mapping) = &def.type_mapping {
                    if let Some(JsonNode::String(val_type)) = obj.get("@type") {
                        if type_mapping == val_type {
                            // Type matches, return simple value
                            return val.clone();
                        }
                    }
                }

                // Check if term's language coercion matches
                if let Some(Some(lang_mapping)) = &def.language_mapping {
                    if let Some(JsonNode::String(val_lang)) = obj.get("@language") {
                        if lang_mapping == val_lang {
                            // Language matches, return simple value
                            return val.clone();
                        }
                    }
                }
            }

            // Can't simplify, return with compacted keys
            let mut result = HashMap::new();
            result.insert("@value".to_string(), val.clone());
            if let Some(t) = obj.get("@type") {
                if let JsonNode::String(type_iri) = t {
                    result.insert(
                        "@type".to_string(),
                        JsonNode::String(self.compact_iri(type_iri, true, false).unwrap_or_else(|| type_iri.clone()))
                    );
                }
            }
            if let Some(l) = obj.get("@language") {
                result.insert("@language".to_string(), l.clone());
            }
            return JsonNode::Object(result);
        }

        // For other objects, just return as-is for now
        // (will be handled by the state machine)
        value.clone()
    }
}

fn to_owned_event(event: JsonEvent<'_>) -> JsonEvent<'static> {
    match event {
        JsonEvent::String(s) => JsonEvent::String(Cow::Owned(s.into_owned())),
        JsonEvent::Number(n) => JsonEvent::Number(Cow::Owned(n.into_owned())),
        JsonEvent::Boolean(b) => JsonEvent::Boolean(b),
        JsonEvent::Null => JsonEvent::Null,
        JsonEvent::StartArray => JsonEvent::StartArray,
        JsonEvent::EndArray => JsonEvent::EndArray,
        JsonEvent::StartObject => JsonEvent::StartObject,
        JsonEvent::EndObject => JsonEvent::EndObject,
        JsonEvent::ObjectKey(k) => JsonEvent::ObjectKey(Cow::Owned(k.into_owned())),
        JsonEvent::Eof => JsonEvent::Eof,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::JsonLdTermDefinition;

    fn create_test_context() -> JsonLdContext {
        let mut context = JsonLdContext::new_empty(None);

        // Add "name" term
        context.term_definitions.insert(
            "name".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/name".to_string())),
                prefix_flag: false,
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

        // Add "schema" prefix
        context.term_definitions.insert(
            "schema".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/".to_string())),
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

        // Add @vocab
        context.vocabulary_mapping = Some("http://example.org/".to_string());

        context
    }

    #[test]
    fn test_compact_iri_exact_term() {
        let context = create_test_context();
        let compactor = JsonLdCompactionConverter::new(context, None);

        assert_eq!(
            compactor.compact_iri("http://schema.org/name", true, false),
            Some("name".to_string())
        );
    }

    #[test]
    fn test_compact_iri_prefix() {
        let context = create_test_context();
        let compactor = JsonLdCompactionConverter::new(context, None);

        assert_eq!(
            compactor.compact_iri("http://schema.org/Person", true, false),
            Some("schema:Person".to_string())
        );
    }

    #[test]
    fn test_compact_iri_vocab() {
        let context = create_test_context();
        let compactor = JsonLdCompactionConverter::new(context, None);

        assert_eq!(
            compactor.compact_iri("http://example.org/foo", true, false),
            Some("foo".to_string())
        );
    }

    #[test]
    fn test_compact_iri_keyword() {
        let context = create_test_context();
        let compactor = JsonLdCompactionConverter::new(context, None);

        assert_eq!(
            compactor.compact_iri("@type", true, false),
            Some("@type".to_string())
        );
    }

    #[test]
    fn test_compact_value_simple() {
        let context = create_test_context();
        let compactor = JsonLdCompactionConverter::new(context, None);

        // Plain value object should be simplified
        let value = JsonNode::Object(
            [("@value".to_string(), JsonNode::String("hello".to_string()))]
                .into_iter()
                .collect()
        );

        assert_eq!(
            compactor.compact_value(None, &value),
            JsonNode::String("hello".to_string())
        );
    }

    #[test]
    fn test_compact_value_with_type() {
        let mut context = create_test_context();

        // Add a term with @type: @id
        context.term_definitions.insert(
            "homepage".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/url".to_string())),
                prefix_flag: false,
                protected: false,
                reverse_property: false,
                base_url: None,
                context: None,
                container_mapping: &[],
                direction_mapping: None,
                index_mapping: None,
                language_mapping: None,
                nest_value: None,
                type_mapping: Some("@id".to_string()),
            },
        );

        let compactor = JsonLdCompactionConverter::new(context, None);

        // Node reference with @type:@id coercion should become simple string
        let value = JsonNode::Object(
            [("@id".to_string(), JsonNode::String("http://example.org/page".to_string()))]
                .into_iter()
                .collect()
        );

        assert_eq!(
            compactor.compact_value(Some("homepage"), &value),
            JsonNode::String("page".to_string())
        );
    }

    #[test]
    fn test_convert_simple_object() {
        let context = create_test_context();

        let input_events = vec![
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("http://schema.org/name")),
            JsonEvent::StartArray,
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@value")),
            JsonEvent::String(Cow::Borrowed("Test")),
            JsonEvent::EndObject,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::Eof,
        ];

        let mut results = Vec::new();
        let mut errors = Vec::new();
        let mut compactor = JsonLdCompactionConverter::new(context, None);

        for event in input_events {
            compactor.convert_event(event, &mut results, &mut errors);
        }

        // Verify "http://schema.org/name" was compacted to "name"
        assert!(results.iter().any(|e| matches!(e, JsonEvent::ObjectKey(k) if k == "name")));
    }

    #[test]
    fn test_compaction_with_graph() {
        // Test compacting an expanded JSON-LD with @graph
        let context = create_test_context();
        let mut compactor = JsonLdCompactionConverter::new(context, None);

        // Expanded input:
        // {"@graph": [{"@id": "http://example.org/item", "http://schema.org/name": [{"@value": "Test"}]}]}
        let input_events = vec![
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@graph")),
            JsonEvent::StartArray,
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@id")),
            JsonEvent::String(Cow::Borrowed("http://example.org/item")),
            JsonEvent::ObjectKey(Cow::Borrowed("http://schema.org/name")),
            JsonEvent::StartArray,
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@value")),
            JsonEvent::String(Cow::Borrowed("Test")),
            JsonEvent::EndObject,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::Eof,
        ];

        let mut results = Vec::new();
        let mut errors = Vec::new();

        for event in input_events {
            compactor.convert_event(event, &mut results, &mut errors);
        }

        assert!(errors.is_empty(), "Errors: {:?}", errors);

        // Verify @graph is preserved and "http://schema.org/name" becomes "name"
        let has_graph = results.iter().any(|e| matches!(e, JsonEvent::ObjectKey(k) if k == "@graph"));
        let has_compacted_name = results.iter().any(|e| matches!(e, JsonEvent::ObjectKey(k) if k == "name"));

        assert!(has_graph, "Should have @graph key");
        assert!(has_compacted_name, "Should have compacted 'name' property");
    }

    #[test]
    fn test_compaction_type_values() {
        // Test that @type values get compacted
        let context = create_test_context();
        let mut compactor = JsonLdCompactionConverter::new(context, None);

        // {"@type": ["http://schema.org/Person"]}
        let input_events = vec![
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@type")),
            JsonEvent::StartArray,
            JsonEvent::String(Cow::Borrowed("http://schema.org/Person")),
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::Eof,
        ];

        let mut results = Vec::new();
        let mut errors = Vec::new();

        for event in input_events {
            compactor.convert_event(event, &mut results, &mut errors);
        }

        // Verify @type value is compacted using prefix
        let has_compacted_type = results.iter().any(|e| {
            matches!(e, JsonEvent::String(s) if s == "schema:Person")
        });

        assert!(has_compacted_type, "Should have compacted type 'schema:Person', got: {:?}", results);
    }

    #[test]
    fn test_serializer_with_inline_context() {
        // Test full serialization + compaction flow with inline context
        use oxrdf::{GraphNameRef, LiteralRef, NamedNodeRef, QuadRef};
        use oxrdf::vocab::rdf;
        use crate::{JsonLdSerializer, JsonNode};

        // Define a context with schema.org prefix
        let context = JsonNode::Object([
            ("schema".to_owned(), JsonNode::String("http://schema.org/".to_owned())),
        ].into_iter().collect());

        let mut serializer = JsonLdSerializer::new()
            .with_context("http://example.org/context", context)
            .for_writer(Vec::new());

        serializer.serialize_quad(QuadRef::new(
            NamedNodeRef::new("http://example.org/article").unwrap(),
            rdf::TYPE,
            NamedNodeRef::new("http://schema.org/Article").unwrap(),
            GraphNameRef::DefaultGraph
        )).unwrap();
        serializer.serialize_quad(QuadRef::new(
            NamedNodeRef::new("http://example.org/article").unwrap(),
            NamedNodeRef::new("http://schema.org/name").unwrap(),
            LiteralRef::new_simple_literal("My Article"),
            GraphNameRef::DefaultGraph
        )).unwrap();

        let output = String::from_utf8(serializer.finish().unwrap()).unwrap();

        // Verify type IRI is compacted
        assert!(output.contains("schema:Article"), "Expected schema:Article in output: {}", output);
        // Verify property IRI is compacted
        assert!(output.contains("schema:name"), "Expected schema:name in output: {}", output);
        // Verify context URL is present
        assert!(output.contains("http://example.org/context"), "Expected context URL in output: {}", output);
    }

    #[test]
    fn test_round_trip_simple() {
        // Test: Create RDF -> serialize to expanded JSON-LD -> compact -> verify structure
        use crate::context::JsonLdTermDefinition;

        // Create a context similar to what RO-Crate uses
        let mut context = JsonLdContext::new_empty(None);

        // Add schema.org terms
        context.term_definitions.insert(
            "schema".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/".to_string())),
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

        context.term_definitions.insert(
            "name".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/name".to_string())),
                prefix_flag: false,
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

        context.term_definitions.insert(
            "Dataset".to_string(),
            JsonLdTermDefinition {
                iri_mapping: Some(Some("http://schema.org/Dataset".to_string())),
                prefix_flag: false,
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

        let mut compactor = JsonLdCompactionConverter::new(context, None);

        // Expanded RO-Crate-like structure
        let input_events = vec![
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@graph")),
            JsonEvent::StartArray,
            // Dataset entity
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@id")),
            JsonEvent::String(Cow::Borrowed("./")),
            JsonEvent::ObjectKey(Cow::Borrowed("@type")),
            JsonEvent::StartArray,
            JsonEvent::String(Cow::Borrowed("http://schema.org/Dataset")),
            JsonEvent::EndArray,
            JsonEvent::ObjectKey(Cow::Borrowed("http://schema.org/name")),
            JsonEvent::StartArray,
            JsonEvent::StartObject,
            JsonEvent::ObjectKey(Cow::Borrowed("@value")),
            JsonEvent::String(Cow::Borrowed("My Research Dataset")),
            JsonEvent::EndObject,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::EndArray,
            JsonEvent::EndObject,
            JsonEvent::Eof,
        ];

        let mut results = Vec::new();
        let mut errors = Vec::new();

        for event in input_events {
            compactor.convert_event(event, &mut results, &mut errors);
        }

        assert!(errors.is_empty(), "Errors: {:?}", errors);

        // Verify key compactions
        let keys: Vec<_> = results.iter().filter_map(|e| {
            if let JsonEvent::ObjectKey(k) = e { Some(k.as_ref()) } else { None }
        }).collect();

        assert!(keys.contains(&"@graph"), "Should have @graph");
        assert!(keys.contains(&"@id"), "Should have @id");
        assert!(keys.contains(&"@type"), "Should have @type");
        assert!(keys.contains(&"name"), "Should have compacted 'name' (not http://schema.org/name)");
    }
}

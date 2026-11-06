package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.{Schema, Reflect}
import zio.blocks.schema.binding.Binding
import zio.blocks.schema.json.{Json, JsonSchema}
import zio.blocks.chunk.{ChunkMap, NonEmptyChunk}

/** Recursive-safe schema export with the public Blocks reflection and schema AST. */
private[pipeline] object StepSchema:
  private def alternatives(values: Vector[JsonSchema]): JsonSchema = NonEmptyChunk.fromIterableOption(values) match
    case Some(values) => JsonSchema.Object(oneOf = Some(values))
    case None => JsonSchema.False
  private def record(fields: Vector[(String, JsonSchema)], required: Set[String]): JsonSchema = JsonSchema.Object(
    `type` = Some(zio.blocks.schema.json.SchemaType.Single(zio.blocks.schema.json.JsonSchemaType.Object)),
    properties = Some(ChunkMap.from(fields)), required = Some(required), additionalProperties = Some(JsonSchema.False))
  def exported[A](schema: Schema[A], namespace: String): Json =
    val ids = scala.collection.mutable.LinkedHashMap.empty[String, String]
    val definitions = scala.collection.mutable.LinkedHashMap.empty[String, JsonSchema]
    def walk(raw: Reflect[Binding, ?]): JsonSchema =
      val value: Reflect[Binding, ?] = raw match
        case deferred: Reflect.Deferred[Binding, ?] => deferred.value
        case other => other
      val key = value.typeId.toString
      ids.get(key) match
        case Some(id) => JsonSchema.refString(s"#/$$defs/$id")
        case None =>
          val id = s"${namespace.replace(".", "_dot_").replace("-", "_dash_")}_${ids.size}"
          ids(key) = id
          val result =
            if value.isPrimitive then Schema(value).toJsonSchema
            else if value.isOption || value.isMaybe then JsonSchema.Object(anyOf = Some(NonEmptyChunk(
              walk(value.optionInnerType.get), JsonSchema.nullSchema)))
            else if value.isRecord then
              val fields = value.asRecord.get.fields
              val required = fields.filter(f => !f.value.isOption && !f.value.isMaybe && !f.value.isCollection && f.value.getDefaultValue.isEmpty).map(_.name).toSet
              record(fields.map(f => f.name -> walk(f.value)).toVector, required)
            else if value.isEnumeration then NonEmptyChunk.fromIterableOption(value.asVariant.get.cases.map(_.name).toVector) match
              case Some(names) => JsonSchema.enumOfStrings(names)
              case None => JsonSchema.False
            else if value.isVariant then alternatives(value.asVariant.get.cases.map(c => record(Vector(c.name -> walk(c.value)), Set(c.name))).toVector)
            else if value.isSequence then JsonSchema.array(walk(value.asSequenceUnknown.get.sequence.element))
            else if value.isMap then JsonSchema.Object(
              `type` = Some(zio.blocks.schema.json.SchemaType.Single(zio.blocks.schema.json.JsonSchemaType.Object)),
              additionalProperties = Some(walk(value.asMapUnknown.get.map.value)))
            else if value.isWrapper then walk(value.asWrapperUnknown.get.wrapper.wrapped)
            else JsonSchema.True
          definitions(id) = result
          JsonSchema.refString(s"#/$$defs/$id")
    walk(schema.reflect) match
      case root: JsonSchema.Object => root.copy($defs = Some(ChunkMap.from(definitions))).toJson
      case other => other.toJson

  /** Embedded schema references resolve at the exported document's root. */
  def hoist(root: Json): Json =
    val definitions = scala.collection.mutable.LinkedHashMap.empty[String, JsonSchema]
    def fields(value: Option[ChunkMap[String, JsonSchema]]): Option[ChunkMap[String, JsonSchema]] =
      value.map(values => ChunkMap.from(values.iterator.map((name, schema) => name -> walk(schema))))
    def walk(value: JsonSchema): JsonSchema = value match
      case obj: JsonSchema.Object =>
        obj.$defs.foreach(_.foreach { (id, definition) => definitions(id) = walk(definition) })
        obj.copy($defs = None, $id = None,
          properties = fields(obj.properties), patternProperties = obj.patternProperties.map(v => ChunkMap.from(v.iterator.map((k, s) => k -> walk(s)))),
          dependentSchemas = fields(obj.dependentSchemas), allOf = obj.allOf.map(_.map(walk)), anyOf = obj.anyOf.map(_.map(walk)),
          oneOf = obj.oneOf.map(_.map(walk)), not = obj.not.map(walk), `if` = obj.`if`.map(walk),
          `then` = obj.`then`.map(walk), `else` = obj.`else`.map(walk), items = obj.items.map(walk),
          prefixItems = obj.prefixItems.map(_.map(walk)), contains = obj.contains.map(walk),
          additionalProperties = obj.additionalProperties.map(walk), propertyNames = obj.propertyNames.map(walk),
          unevaluatedProperties = obj.unevaluatedProperties.map(walk), unevaluatedItems = obj.unevaluatedItems.map(walk),
          contentSchema = obj.contentSchema.map(walk))
      case other => other
    val parsed = JsonSchema.fromJson(root).fold(error => throw IllegalArgumentException(error.message), identity)
    walk(parsed) match
      case body: JsonSchema.Object => body.copy($defs = Some(ChunkMap.from(definitions))).toJson
      case other => other.toJson

package io.github.adrielc.kyopdf.pipeline

import zio.blocks.schema.{Schema, Reflect}
import zio.blocks.schema.binding.Binding
import zio.blocks.schema.json.Json
import Json.{Object as Obj, Array as Arr, String as Str, Boolean as Bool}

/** Recursive-safe export from the same public Blocks reflection used by the
  * codecs. The pinned codec's toJsonSchema waits recursively on Deferred types.
  * Reserve a definition before descending instead of invoking that code on a
  * recursive record. Primitive constraints still use the upstream exporter.
  */
private[pipeline] object StepSchema:
  def exported[A](schema: Schema[A], namespace: String): Json =
    val ids = scala.collection.mutable.LinkedHashMap.empty[String, String]
    val definitions = scala.collection.mutable.LinkedHashMap.empty[String, Json]
    def walk(raw: Reflect[Binding, ?]): Json =
      val value: Reflect[Binding, ?] = raw match
        case deferred: Reflect.Deferred[Binding, ?] => deferred.value
        case other => other
      val key = value.typeId.toString
      ids.get(key) match
        case Some(id) => Obj("$ref" -> Str(s"#/$$defs/$id"))
        case None =>
          val id = s"${namespace.replace(".", "_dot_").replace("-", "_dash_")}_${ids.size}"
          ids(key) = id
          val result: Json =
            if value.isPrimitive then Schema(value).toJsonSchema.toJson
            else if value.isOption || value.isMaybe then Obj("anyOf" -> Arr(walk(value.optionInnerType.get), Obj("type" -> Str("null"))))
            else if value.isRecord then
              val fields = value.asRecord.get.fields
              val required = fields.filter(f => !f.value.isOption && !f.value.isMaybe && !f.value.isCollection && f.value.getDefaultValue.isEmpty)
              Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
                "required" -> Arr(required.map(f => Str(f.name))*),
                "properties" -> Obj(fields.map(f => f.name -> walk(f.value))*))
            else if value.isEnumeration then Obj("enum" -> Arr(value.asVariant.get.cases.map(c => Str(c.name))*))
            else if value.isVariant then Obj("oneOf" -> Arr(value.asVariant.get.cases.map { c =>
              Obj("type" -> Str("object"), "additionalProperties" -> Bool(false),
                "required" -> Arr(Str(c.name)), "properties" -> Obj(c.name -> walk(c.value)))
            }*))
            else if value.isSequence then Obj("type" -> Str("array"), "items" -> walk(value.asSequenceUnknown.get.sequence.element))
            else if value.isMap then Obj("type" -> Str("object"), "additionalProperties" -> walk(value.asMapUnknown.get.map.value))
            else if value.isWrapper then walk(value.asWrapperUnknown.get.wrapper.wrapped)
            else Bool(true)
          definitions(id) = result
          Obj("$ref" -> Str(s"#/$$defs/$id"))
    val root = walk(schema.reflect)
    Obj((root.asInstanceOf[Obj].value.toVector :+ ("$defs" -> Obj(definitions.toVector*)))*)

  /** References in embedded schemas are rooted at the exported document. */
  def hoist(root: Json): Json =
    val definitions = scala.collection.mutable.LinkedHashMap.empty[String, Json]
    def walk(value: Json): Json = value match
      case obj: Obj =>
        obj.value.find(_._1 == "$defs").foreach { (_, value) => value match
          case defs: Obj => defs.value.foreach { (id, definition) =>
            definitions(id) = walk(definition)
          }
          case _ => ()
        }
        Obj(obj.value.toVector.filterNot((key, _) => key == "$defs" || key == "$id").map((key, field) => key -> walk(field))*)
      case Arr(values) => Arr(values.map(walk).toSeq*)
      case other => other
    val body = walk(root).asInstanceOf[Obj]
    Obj((body.value.toVector :+ ("$defs" -> Obj(definitions.toVector*)))*)

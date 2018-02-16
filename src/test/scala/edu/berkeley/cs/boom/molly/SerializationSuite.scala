package edu.berkeley.cs.boom.molly

import org.scalactic.Explicitly
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.tags.Slow
import org.scalatest.{ FlatSpec, Matchers, PropSpec }
import java.io.File
import com.codahale.metrics.MetricRegistry
import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._
import Explicitly._
import edu.berkeley.cs.boom.molly.report._
import scala.collection.mutable.{ ListBuffer, HashMap }

class SerializationSuite extends PropSpec with TableDrivenPropertyChecks with Matchers {

  // Parsing of provenance JSON with Play JSON library.

  case class ProvData(
    goals: ListBuffer[Node],
    rules: ListBuffer[Node],
    edges: ListBuffer[Edge])

  case class DotProvData(
    goals: HashMap[String, Boolean],
    rules: HashMap[String, Boolean],
    edges: HashMap[String, Boolean])

  case class Node(
    id: String,
    label: String = "",
    table: String = "")

  case class Edge(
    from: String,
    to: String)

  implicit val nodeReads: Reads[Node] = (
    (JsPath \ "id").read[String] and
    (JsPath \ "label").read[String] and
    (JsPath \ "table").read[String]
  )(Node.apply _)

  implicit val edgeReads: Reads[Edge] = (
    (JsPath \ "from").read[String] and
    (JsPath \ "to").read[String]
  )(Edge.apply _)

  implicit val provDataReads: Reads[ProvData] = (
    (JsPath \ "goals").read[ListBuffer[Node]] and
    (JsPath \ "rules").read[ListBuffer[Node]] and
    (JsPath \ "edges").read[ListBuffer[Edge]]
  )(ProvData.apply _)

  val examplesFTPath = SyncFTChecker.getClass.getClassLoader.getResource("examples_ft").getPath

  // Parsing of dot files.

  private def parseDot(input: String): DotProvData = {

    val regStart = "digraph\\sdataflow\\s\\{\\s{0,}\\{rank=\"same\";\\s(goal[0-9]+,\\s){0,}goal([0-9]+)\\}\\s{0,}".r
    val regLine = "\\s\\[.+\\];\n\\s{4}".r

    // Remove graph definition at beginning
    // and final newline and closing curly bracket.
    val cleanedInputStart = regStart.replaceAllIn(input, "").replaceAll("\n}", "\n    ")
    val cleanedInput = regLine.replaceAllIn(cleanedInputStart, "\n")
    val lines = cleanedInput.split("\\n");

    val provData = new DotProvData(goals = new HashMap[String, Boolean](), rules = new HashMap[String, Boolean](), edges = new HashMap[String, Boolean]())

    for (line <- lines) {

      if (line.contains("->")) {
        provData.edges += (line -> true)
      } else {

        if (line.contains("goal")) {
          provData.goals += (line -> true)
        } else {
          provData.rules += (line -> true)
        }
      }
    }

    provData
  }

  // Scenarios table holding all test cases.

  val scenarios = Table(
    ("input programs", "eot", "eff", "crashes", "nodes", "allCounter", "negativeSupport"),
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), false, false),
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), true, false),
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), false, true),
    (Seq("delivery/simplog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), true, true),
    (Seq("delivery/rdlog.ded", "delivery/deliv_assert.ded"), 5, 2, 0, Seq("a", "b", "c"), true, true),
    (Seq("delivery/rdlog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), true, true),
    (Seq("delivery/classic_rb.ded", "delivery/deliv_assert.ded"), 5, 2, 0, Seq("a", "b", "c"), true, true),
    (Seq("delivery/classic_rb.ded", "delivery/deliv_assert.ded"), 5, 0, 2, Seq("a", "b", "c"), true, true),
    (Seq("delivery/replog.ded", "delivery/deliv_assert.ded"), 5, 2, 0, Seq("a", "b", "c"), true, true),
    (Seq("delivery/replog.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), true, true),
    (Seq("delivery/ack_rb.ded", "delivery/deliv_assert.ded"), 5, 2, 1, Seq("a", "b", "c"), true, true),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"), 5, 2, 0, Seq("a", "b", "C", "d"), true, true),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"), 5, 2, 1, Seq("a", "b", "C", "d"), true, true),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"), 5, 0, 1, Seq("a", "b", "C", "d"), true, true),
    (Seq("commit/2pc.ded", "commit/2pc_assert.ded"), 5, 0, 2, Seq("a", "b", "C", "d"), true, true),
    (Seq("commit/2pc.ded", "commit/2pc_assert_optimist.ded"), 6, 0, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc.ded", "commit/2pc_assert_optimist.ded"), 6, 0, 2, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert_optimist.ded"), 6, 0, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert_optimist.ded"), 6, 0, 2, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert.ded"), 6, 0, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_timeout.ded", "commit/2pc_assert.ded"), 6, 0, 2, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_ctp.ded", "commit/2pc_assert.ded"), 6, 0, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/2pc_ctp.ded", "commit/2pc_assert.ded"), 6, 0, 2, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"), 8, 0, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"), 8, 0, 2, Seq("a", "b", "C", "d"), true, false),
    (Seq("commit/3pc.ded", "commit/2pc_assert.ded"), 9, 7, 1, Seq("a", "b", "C", "d"), true, false),
    (Seq("kafka.ded"), 7, 4, 1, Seq("a", "b", "c", "C", "Z"), true, false),
    (Seq("kafka.ded"), 7, 4, 0, Seq("a", "b", "c", "C", "Z"), true, false)
  )

  // Main test.

  property("Enabling provenance output should produce correct JSON-serialized provenance graphs") {

    forAll(scenarios) {

      (inputPrograms: Seq[String], eot: Int, eff: Int, crashes: Int, nodes: Seq[String], allCounter: Boolean, negativeSupport: Boolean) =>

        val inputFiles = inputPrograms.map(name => new File(examplesFTPath, name))
        val config = Config(eot, eff, crashes, nodes, inputFiles, generateProvenanceDiagrams = true, findAllCounterexamples = allCounter, negativeSupport = negativeSupport)
        val metrics = new MetricRegistry
        val results = SyncFTChecker.check(config, metrics)

        results.foreach {
          r =>
            {
              val dotProv = ProvenanceDiagramGenerator.generateDot(r.provenance)
              val jsonProv = Json.parse(ProvenanceDiagramGenerator.generateJSON(r.provenance))

              val parsedDot: DotProvData = parseDot(dotProv)
              val parsedJSON: ProvData = jsonProv.as[ProvData]

              parsedJSON.goals should have size parsedDot.goals.size
              parsedJSON.rules should have size parsedDot.rules.size
              parsedJSON.edges should have size parsedDot.edges.size

              for (goal <- parsedJSON.goals) {

                if (parsedDot.goals.contains(goal.id)) {
                  parsedDot.goals -= goal.id
                  parsedJSON.goals -= goal
                } else {
                  fail(s"Did not find goal '${goal.id}' in dot data.")
                }
              }

              for (rule <- parsedJSON.rules) {

                if (parsedDot.rules.contains(rule.id)) {
                  parsedDot.rules -= rule.id
                  parsedJSON.rules -= rule
                } else {
                  fail(s"Did not find rule '${rule.id}' in dot data.")
                }
              }

              for (edge <- parsedJSON.edges) {

                val edgeKey: String = s"${edge.from} -> ${edge.to}"

                if (parsedDot.edges.contains(edgeKey)) {
                  parsedDot.edges -= edgeKey
                  parsedJSON.edges -= edge
                } else {
                  fail(s"Did not find edge '${edgeKey}' in dot data.")
                }
              }

              parsedJSON.goals should have size parsedDot.goals.size
              parsedJSON.rules should have size parsedDot.rules.size
              parsedJSON.edges should have size parsedDot.edges.size
            }
        }

        results should not be empty
    }
  }
}

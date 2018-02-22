package edu.berkeley.cs.boom.molly.report

import scala.io.Source
import scala.sys.process._
import java.util.concurrent.Executors
import scala.collection.mutable.{ Queue, HashMap }
import java.io.{ File, FileWriter, FileOutputStream }
import edu.berkeley.cs.boom.molly.derivations.{ DerivationTreeNode, RuleNode, GoalNode }

object ProvenanceDiagramGenerator extends GraphvizPrettyPrinter {

  private val GRAY = "gray"
  private val BLACK = "black"
  private val WHITE = "white"

  private def fontColor(node: DerivationTreeNode): String = {
    node match {
      case goal: GoalNode =>
        if (goal.tuple.tombstone) GRAY
        else if (goal.tuple.negative) WHITE
        else BLACK
      case rule: RuleNode =>
        if (rule.subgoals.exists(_.tuple.tombstone)) GRAY
        else BLACK
    }
  }

  private def fillColor(node: DerivationTreeNode): String = {
    node match {
      case goal: GoalNode =>
        if (goal.tuple.tombstone) GRAY
        else if (goal.tuple.negative) BLACK
        else WHITE
      case rule: RuleNode =>
        if (rule.subgoals.exists(_.tuple.tombstone)) GRAY
        else WHITE
    }
  }

  private def color(node: DerivationTreeNode): String = {
    node match {
      case goal: GoalNode =>
        if (goal.tuple.tombstone) GRAY
        else BLACK
      case rule: RuleNode =>
        if (rule.subgoals.exists(_.tuple.tombstone)) GRAY
        else BLACK
    }
  }

  /**
   * generateAndWriteDot
   */
  def generateAndWriteDot(goals: List[GoalNode], outDir: File, iteration: Int): Runnable = {

    val goalsFileName = new File(outDir, s"run_${iteration}_provenance_goals.partial.dot")
    val rulesFileName = new File(outDir, s"run_${iteration}_provenance_rules.partial.dot")
    val edgesFileName = new File(outDir, s"run_${iteration}_provenance_edges.partial.dot")
    val provFileName = new File(outDir, s"run_${iteration}_provenance.dot")
    val svgFileName = new File(outDir, s"run_${iteration}_provenance.svg")

    // Open a new file for goals, rules, and edges
    // with the append flag set to true.
    val goalsFile = new FileWriter(goalsFileName, true)
    val rulesFile = new FileWriter(rulesFileName, true)
    val edgesFile = new FileWriter(edgesFileName, true)

    // Keep track of elements we already saw before
    // in order to avoid duplicates.
    var seenGoals = new HashMap[String, Boolean]()
    var seenRules = new HashMap[String, Boolean]()
    var seenEdges = new HashMap[String, Boolean]()

    // Create a queue for all goal and rule nodes
    // and enqueue all goals from the supplied
    // argument first.
    var nodeQ: Queue[DerivationTreeNode] = Queue(goals: _*)

    while (!nodeQ.isEmpty) {

      val cur: DerivationTreeNode = nodeQ.dequeue

      try {

        cur match {

          case goal: GoalNode => {

            // Create provenance for this goal node.
            val nodeID = s"goal${goal.id}"
            val goalMapKey = s"${nodeID},${goal.tuple.toString}"

            if (!seenGoals.contains(goalMapKey)) {

              val goalData = super.pretty(
                nest(linebreak <>
                  node(
                    nodeID,
                    "label" -> goal.tuple.toString,
                    "style" -> (if (goal.tuple.tombstone) "dashed" else "filled"),
                    "fontcolor" -> fontColor(goal),
                    "color" -> color(goal),
                    "fillcolor" -> fillColor(goal)
                  )
                )
              )

              // Append to goals files and flush to disk.
              goalsFile.write(goalData)
              goalsFile.flush()

              // Add goal node to hash map of seen items.
              seenGoals += (goalMapKey -> true)
            }

            for (rule <- goal.rules) {

              val edgeMapKey = s"${nodeID}->rule${rule.id}"

              if (!seenEdges.contains(edgeMapKey)) {

                val edgeData = super.pretty(
                  nest(linebreak <>
                    diEdge(nodeID, "rule" + rule.id, "color" -> (if (rule.subgoals.exists(_.tuple.tombstone)) GRAY else BLACK))
                  )
                )

                // Append to edges files and flush to disk.
                edgesFile.write(edgeData)
                edgesFile.flush()

                // Add edge to hash map of seen items.
                seenEdges += (edgeMapKey -> true)
              }

              // Queue all subrules.
              nodeQ.enqueue(rule)
            }
          }

          case rule: RuleNode => {

            // Create provenance for this rule node.
            val nodeID = s"rule${rule.id}"
            val ruleMapKey = s"${nodeID},${rule.rule.head.tableName}"

            if (!seenRules.contains(ruleMapKey)) {

              val ruleData = super.pretty(
                nest(linebreak <>
                  node(nodeID,
                    "label" -> rule.rule.head.tableName,
                    "shape" -> "rect",
                    "fontcolor" -> fontColor(rule),
                    "color" -> color(rule),
                    "fillcolor" -> fillColor(rule)
                  )
                )
              )

              // Append to rules files and flush to disk.
              rulesFile.write(ruleData)
              rulesFile.flush()

              // Add rule node to hash map of seen items.
              seenRules += (ruleMapKey -> true)
            }

            for (goal <- rule.subgoals) {

              val edgeMapKey = s"${nodeID}->goal${goal.id}"

              if (!seenEdges.contains(edgeMapKey)) {

                val edgeData = super.pretty(
                  nest(linebreak <>
                    diEdge(nodeID, "goal" + goal.id, "color" -> (if (goal.tuple.tombstone) GRAY else BLACK))
                  )
                )

                // Append to edges files and flush to disk.
                edgesFile.write(edgeData)
                edgesFile.flush()

                // Add edge to hash map of seen items.
                seenEdges += (edgeMapKey -> true)
              }

              // Queue all subgoals.
              nodeQ.enqueue(goal)
            }
          }
        }

      } catch {

        case t: Throwable => {
          // Close any open file descriptor.
          goalsFile.close()
          rulesFile.close()
          edgesFile.close()
        }
      }
    }

    // Close file descriptor.
    goalsFile.close()
    rulesFile.close()
    edgesFile.close()

    // Open final dot provenance file.
    val provFile = new FileWriter(provFileName, true)

    provFile.write("digraph dataflow {\n    {rank=\"same\"; " + goals.map(g => "goal" + g.id).mkString(", ") + "}\n")
    provFile.flush()

    // Read back from goals files.
    val goalsFD = Source.fromFile(goalsFileName)

    for (line <- goalsFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.flush()
    goalsFD.close

    // Read back from rules files.
    val rulesFD = Source.fromFile(rulesFileName)

    for (line <- rulesFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.flush()
    rulesFD.close

    // Read back from edges files.
    val edgesFD = Source.fromFile(edgesFileName)

    for (line <- edgesFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.write("}\n")
    provFile.flush()
    edgesFD.close

    // Remove all partial files.
    goalsFileName.delete()
    rulesFileName.delete()
    edgesFileName.delete()

    new Runnable() {

      def run() {
        val dotExitCode = s"dot -Tsvg -o ${svgFileName.getAbsolutePath} ${provFileName.getAbsolutePath}".!
        assert(dotExitCode == 0)
      }
    }
  }

  /**
   * generateAndWriteJSON
   */
  def generateAndWriteJSON(goals: List[GoalNode], outDir: File, iteration: Int) = {

    val goalsFileName = new File(outDir, s"run_${iteration}_provenance_goals.partial.json")
    val rulesFileName = new File(outDir, s"run_${iteration}_provenance_rules.partial.json")
    val edgesFileName = new File(outDir, s"run_${iteration}_provenance_edges.partial.json")
    val provFileName = new File(outDir, s"run_${iteration}_provenance.json")

    // Open a new file for goals, rules, and edges
    // with the append flag set to true.
    val goalsFile = new FileWriter(goalsFileName, true)
    val rulesFile = new FileWriter(rulesFileName, true)
    val edgesFile = new FileWriter(edgesFileName, true)

    // Keep track of elements we already saw before
    // in order to avoid duplicates.
    var seenGoals = new HashMap[String, Boolean]()
    var seenRules = new HashMap[String, Boolean]()
    var seenEdges = new HashMap[String, Boolean]()

    // Create a queue for all goal and rule nodes
    // and enqueue all goals from the supplied
    // argument first.
    var nodeQ: Queue[DerivationTreeNode] = Queue(goals: _*)

    while (!nodeQ.isEmpty) {

      val cur: DerivationTreeNode = nodeQ.dequeue

      try {

        cur match {

          case goal: GoalNode => {

            // Create provenance for this goal node.
            val nodeID = s"goal${goal.id}"
            val goalMapKey = s"${nodeID},${goal.tuple.toString}"

            if (!seenGoals.contains(goalMapKey)) {

              val goalData = super.pretty(
                nest(
                  nest(linebreak <>
                    braces(
                      nest(linebreak <>
                        "\"id\": \"" + nodeID + "\"" <> comma <> linebreak <>
                        "\"label\": \"" + goal.tuple.toString + "\"" <> comma <> linebreak <>
                        "\"table\": \"" + goal.tuple.table + "\""
                      ) <> linebreak
                    ) <> comma
                  )
                )
              )

              // Append to goals files and flush to disk.
              goalsFile.write(goalData)
              goalsFile.flush()

              // Add goal node to hash map of seen items.
              seenGoals += (goalMapKey -> true)
            }

            for (rule <- goal.rules) {

              val edgeMapKey = s"${nodeID}->rule${rule.id}"

              if (!seenEdges.contains(edgeMapKey)) {

                val edgeData = super.pretty(
                  nest(
                    nest(linebreak <>
                      braces(
                        nest(linebreak <>
                          "\"from\":" <+> "\"" <> text(nodeID) <> "\"" <> comma <> linebreak <>
                          "\"to\":" <+> "\"" <> text("rule" + rule.id) <> "\""
                        ) <> linebreak
                      ) <> comma
                    )
                  )
                )

                // Append to edges files and flush to disk.
                edgesFile.write(edgeData)
                edgesFile.flush()

                // Add edge to hash map of seen items.
                seenEdges += (edgeMapKey -> true)
              }

              // Queue all subrules.
              nodeQ.enqueue(rule)
            }
          }

          case rule: RuleNode => {

            // Create provenance for this rule node.
            val nodeID = s"rule${rule.id}"
            val ruleMapKey = s"${nodeID},${rule.rule.head.tableName}"

            if (!seenRules.contains(ruleMapKey)) {

              val ruleData = super.pretty(
                nest(
                  nest(linebreak <>
                    braces(
                      nest(linebreak <>
                        "\"id\": \"" + nodeID + "\"" <> comma <> linebreak <>
                        "\"label\": \"" + rule.rule.head.tableName + "\"" <> comma <> linebreak <>
                        "\"table\": \"" + rule.rule.head.tableName.split("_")(0) + "\""
                      ) <> linebreak
                    ) <> comma
                  )
                )
              )

              // Append to rules files and flush to disk.
              rulesFile.write(ruleData)
              rulesFile.flush()

              // Add rule node to hash map of seen items.
              seenRules += (ruleMapKey -> true)
            }

            for (goal <- rule.subgoals) {

              val edgeMapKey = s"${nodeID}->goal${goal.id}"

              if (!seenEdges.contains(edgeMapKey)) {

                val edgeData = super.pretty(
                  nest(
                    nest(linebreak <>
                      braces(
                        nest(linebreak <>
                          "\"from\":" <+> "\"" <> text(nodeID) <> "\"" <> comma <> linebreak <>
                          "\"to\":" <+> "\"" <> text("goal" + goal.id) <> "\""
                        ) <> linebreak
                      ) <> comma
                    )
                  )
                )

                // Append to edges files and flush to disk.
                edgesFile.write(edgeData)
                edgesFile.flush()

                // Add edge to hash map of seen items.
                seenEdges += (edgeMapKey -> true)
              }

              // Queue all subgoals.
              nodeQ.enqueue(goal)
            }
          }
        }

      } catch {

        case t: Throwable => {
          // Close any open file descriptor.
          goalsFile.close()
          rulesFile.close()
          edgesFile.close()
        }
      }
    }

    // Close file descriptor.
    goalsFile.close()
    rulesFile.close()
    edgesFile.close()

    // Get file sizes in bytes of all partial files.
    val goalsPartialLen: Long = goalsFileName.length() - 1
    val rulesPartialLen: Long = rulesFileName.length() - 1
    val edgesPartialLen: Long = edgesFileName.length() - 1

    // Truncate all partial files by their size - 1 (removing trailing ',').
    val goalsPartialStream = new FileOutputStream(goalsFileName, true).getChannel()
    val rulesPartialStream = new FileOutputStream(rulesFileName, true).getChannel()
    val edgesPartialStream = new FileOutputStream(edgesFileName, true).getChannel()
    goalsPartialStream.truncate(goalsPartialLen)
    rulesPartialStream.truncate(rulesPartialLen)
    edgesPartialStream.truncate(edgesPartialLen)
    goalsPartialStream.close()
    rulesPartialStream.close()
    edgesPartialStream.close()

    // Open final JSON provenance file.
    val provFile = new FileWriter(provFileName, true)

    provFile.write("{\n    \"goals\": [")
    provFile.flush()

    // Read back from goals files.
    val goalsFD = Source.fromFile(goalsFileName)

    for (line <- goalsFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.write("    ],\n    \"rules\": [")
    provFile.flush()
    goalsFD.close

    // Read back from rules files.
    val rulesFD = Source.fromFile(rulesFileName)

    for (line <- rulesFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.write("    ],\n    \"edges\": [")
    provFile.flush()
    rulesFD.close

    // Read back from edges files.
    val edgesFD = Source.fromFile(edgesFileName)

    for (line <- edgesFD.getLines) {
      provFile.write(line)
      provFile.write("\n")
    }

    provFile.write("    ]\n}\n")
    provFile.flush()
    edgesFD.close

    // Remove all partial files.
    goalsFileName.delete()
    rulesFileName.delete()
    edgesFileName.delete()
  }
}

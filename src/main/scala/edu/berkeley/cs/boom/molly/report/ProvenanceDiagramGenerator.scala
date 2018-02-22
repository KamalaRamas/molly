package edu.berkeley.cs.boom.molly.report

import java.io.{ File, FileWriter, FileOutputStream }
import scala.io.Source
import scala.collection.mutable.{ Queue, HashMap }
import edu.berkeley.cs.boom.molly.derivations.{ DerivationTreeNode, RuleNode, GoalNode }

object ProvenanceDiagramGenerator extends GraphvizPrettyPrinter {

  private val GRAY = "gray"
  private val BLACK = "black"
  private val WHITE = "white"

  def generateDot(goals: List[GoalNode]): String = {

    val dot = "digraph" <+> "dataflow" <+> braces(nest(
      linebreak <>
        braces("rank=\"same\";" <+> ssep(goals.map(g => text("goal" + g.id)), comma <> space)) <@@>
        // Converting to a set of strings is an ugly trick to avoid adding duplicate edges:
        goals.flatMap(dotStatements).map(d => super.pretty(d)).toSet.map(text).foldLeft(empty)(_ <@@> _)
    ) <> linebreak) <> linebreak

    super.pretty(dot)
  }

  /**
   * generateAndWriteJSON
   */
  def generateAndWriteJSON(goals: List[GoalNode], outDir: File, iteration: Int) = {

    val goalsFileName = new File(outDir, s"run_${iteration}_provenance_goals.partial")
    val rulesFileName = new File(outDir, s"run_${iteration}_provenance_rules.partial")
    val edgesFileName = new File(outDir, s"run_${iteration}_provenance_edges.partial")
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
            }

            // Queue all subrules.
            for (rule <- goal.rules) {
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
            }

            // Queue all subgoals.
            for (goal <- rule.subgoals) {
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

    val ret1 = goalsPartialStream.truncate(goalsPartialLen)
    val ret2 = rulesPartialStream.truncate(rulesPartialLen)
    val ret3 = edgesPartialStream.truncate(edgesPartialLen)

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

  private def dotStatements(goal: GoalNode): List[Doc] = {

    val id = "goal" + goal.id

    val goalNode = node(id,
      "label" -> goal.tuple.toString,
      "style" -> (if (goal.tuple.tombstone) "dashed" else "filled"),
      "fontcolor" -> fontColor(goal),
      "color" -> color(goal),
      "fillcolor" -> fillColor(goal))

    val edges = goal.rules.map {
      rule => diEdge(id, "rule" + rule.id, "color" -> (if (rule.subgoals.exists(_.tuple.tombstone)) GRAY else BLACK))
    }

    List(goalNode) ++ edges ++ goal.rules.flatMap(dotStatements)
  }

  private def dotStatements(rule: RuleNode): List[Doc] = {

    val id = "rule" + rule.id

    val nodes = List(node(id,
      "label" -> rule.rule.head.tableName,
      "shape" -> "rect",
      "fontcolor" -> fontColor(rule),
      "color" -> color(rule),
      "fillcolor" -> fillColor(rule)))

    val edges = rule.subgoals.map {
      goal => diEdge(id, "goal" + goal.id, "color" -> (if (goal.tuple.tombstone) GRAY else BLACK))
    }

    nodes ++ edges ++ rule.subgoals.flatMap(dotStatements)
  }
}

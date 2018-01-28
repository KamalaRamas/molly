package edu.berkeley.cs.boom.molly.report

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
    ) <> linebreak)

    super.pretty(dot)
  }

  def generateJSON(goals: List[GoalNode]): String = {

    val data = goals.flatMap(jsonStatements)

    // Filter out nodes and edges from the full list.
    val nodes = data.filter((item: String) => item.contains("label")).toSet
    val edges = data.filter((item: String) => !item.contains("label")).toSet

    // Construct the final JSON.
    val json = braces(
      nest(linebreak <>
        "\"nodes\":" <+> brackets(
          nest(linebreak <>
            nest(
              ssep(nodes.map(
                node =>
                  braces(linebreak <>
                    nest(text(node))
                  )
              ).to[scala.collection.immutable.Seq], comma <> linebreak)
            ) <> linebreak
          )
        ) <> comma <> linebreak <>
          "\"edges\":" <+> brackets(
            nest(linebreak <>
              nest(
                ssep(edges.map(
                  edge =>
                    braces(linebreak <>
                      nest(text(edge))
                    )
                ).to[scala.collection.immutable.Seq], comma <> linebreak)
              ) <> linebreak
            )
          )
      ) <> linebreak
    ) <> linebreak

    super.pretty(json)
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

  private def jsonStatements(goal: GoalNode): List[String] = {

    val nodeID = s"goal${goal.id}"

    val goalString = super.pretty(
      "\"" + nodeID + "\":" <+> braces(
        nest(linebreak <>
          nest(
            nest(
              nest(
                "\"label\":" <+> "\"" + goal.tuple.toString + "\"" <> linebreak
              )
            )
          )
        )
      ) <> linebreak
    )

    val edges = goal.rules.map {
      rule =>
        super.pretty(
          nest(
            nest(
              nest(
                "\"from\":" <+> "\"" <> text(nodeID) <> "\"" <> comma <> linebreak <>
                  "\"to\":" <+> "\"" <> text("rule" + rule.id) <> "\""
              ) <> linebreak
            )
          )
        )
    }

    List(goalString) ++ edges ++ goal.rules.flatMap(jsonStatements)
  }

  private def jsonStatements(rule: RuleNode): List[String] = {

    val nodeID = s"rule${rule.id}"

    val ruleString = super.pretty(
      "\"" + nodeID + "\":" <+> braces(
        nest(linebreak <>
          nest(
            nest(
              nest(
                "\"label\":" <+> "\"" + rule.rule.head.tableName + "\"" <> linebreak
              )
            )
          )
        )
      ) <> linebreak
    )

    val edges = rule.subgoals.map {
      goal =>
        super.pretty(
          nest(
            nest(
              nest(
                "\"from\":" <+> "\"" <> text(nodeID) <> "\"" <> comma <> linebreak <>
                  "\"to\":" <+> "\"" <> text("goal" + goal.id) <> "\""
              ) <> linebreak
            )
          )
        )
    }

    List(ruleString) ++ edges ++ rule.subgoals.flatMap(jsonStatements)
  }
}

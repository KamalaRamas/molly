package edu.berkeley.cs.boom.molly

import edu.berkeley.cs.boom.molly.derivations._

// TODO: this class is misleadingly named, since it holds the contents of ALL tables
// at ALL timesteps.
case class UltimateModel(tables: Map[String, List[List[String]]]) {

  override def toString: String = {
    tables.map {
      case (name, values) =>
        name + ":\n" + values.map(_.mkString(",")).mkString("\n")
    }.mkString("\n\n")
  }

  def getAllTuples(): List[GoalTuple] = {
    tables.map {
      case (t, vals) => vals.map(GoalTuple(t, _))
    }.filter(goalList => !goalList.isEmpty).flatten.toList.sortWith(_.cols.last.toInt > _.cols.last.toInt)
  }

  def tableAtTime(table: String, time: Int) = tables(table).filter(_.last.toInt == time)
}

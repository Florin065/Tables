import util.Util.Row
import util.Util.Line
import TestTables.tableImperative
import TestTables.tableFunctional
import TestTables.tableObjectOriented

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName).map(predicate)
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    f1.eval(r).flatMap(b1 => f2.eval(r).map(b2 => b1 && b2))
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    f1.eval(r).flatMap(b1 => f2.eval(r).map(b2 => b1 || b2))
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = ???
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = ???
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = ???
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val sb = new StringBuilder()
    sb.append(columnNames.mkString(","))
    tabular.foreach(row => {
      sb.append("\n")
      sb.append(row.mkString(","))
    })
    sb.toString()
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    val newTabular = tabular.map(row => {
      columns.flatMap(col => {
        val index = columnNames.indexOf(col)
        if (index == -1) {
          None
        } else {
          Some(row(index))
        }
      })
    })
    val newColumnNames = columns.filter(col => columnNames.contains(col))
    if (newColumnNames.length == columns.length) {
      Some(new Table(newColumnNames, newTabular))
    } else {
      None
    }
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    cond match {
      case Field(colName, predicate) => {
        val index = columnNames.indexOf(colName)
        if (index == -1) {
          None
        } else {
          val newTabular = tabular.filter(row => predicate(row(index)))
          Some(new Table(columnNames, newTabular))
        }
      }
      case And(f1, f2) => {
        for {
          t1 <- filter(f1)
          t2 <- filter(f2)
        } yield new Table(t1.getColumnNames, t1.getTabular.intersect(t2.getTabular))
      }
      case Or(f1, f2) => {
        for {
          t1 <- filter(f1)
          t2 <- filter(f2)
        } yield new Table(t1.getColumnNames, t1.getTabular.concat(t2.getTabular))
      }
    }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = ???

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = ???
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lines = s.split("\n").toList
    val columnNames = lines.head.split(",").toList
    val tabular = lines.tail.map(_.split(",").toList)
    new Table(columnNames, tabular)
  }
}

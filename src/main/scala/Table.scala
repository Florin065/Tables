import util.Util.Row
import util.Util.Line

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = f1.eval(r).flatMap(b1 => f2.eval(r).map(b2 => b1 && b2))
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = f1.eval(r).flatMap(b1 => f2.eval(r).map(b2 => b1 || b2))
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Option(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = target.eval.get.select(columns)
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = target.eval.get.filter(condition)
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = Option(target.eval.get.newCol(name, defaultVal))
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = t1.eval.get.merge(key, t2.eval.get)
}

class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames :               Line = columnNames
  def     getTabular : List[List[String]] =     tabular

  // 1.1
  override def toString: String = (columnNames :: tabular).map(_.mkString(",")).mkString("\n")

  // 2.1
  def select(columns: Line): Option[Table] = {
    val newColumnNames = columnNames.filter(columns.contains)

    val newTabular = for {
      row <- tabular
      newValues = for {
        col <- columns
        index = columnNames.indexOf(col)
        if index != -1
      } yield row(index)
    } yield newValues

    Option.when(newColumnNames.length == columns.length)(new Table(columns, newTabular))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = cond match {
    case Field(colName, predicate) =>
      columnNames.indexOf(colName) match {
        case -1 => Option.empty
        case index => Option(new Table(columnNames, tabular.filter(row => predicate(row(index)))))
      }
    case And(f1, f2) =>
      filter(f1).flatMap(t1 => filter(f2).map(t2 => new Table(t1.getColumnNames, t1.getTabular intersect t2.getTabular)))
    case Or(f1, f2) =>
      filter(f1).flatMap(t1 => filter(f2).map(t2 => new Table(t1.getColumnNames, (t2.getTabular ++ t1.getTabular).distinct)))
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = new Table(columnNames :+ name, tabular.map(_ :+ defaultVal))

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    if (!getColumnNames.contains(key) || !other.getColumnNames.contains(key)) return Option.empty

    val mergedColumns = (getColumnNames ++ other.getColumnNames).distinct

    val mergedRows = getTabular.flatMap { row1 =>
      other.getTabular.filter(row2 => row1(getColumnNames.indexOf(key)) == row2(other.getColumnNames.indexOf(key)))
        .map { row2 =>
          mergedColumns.map { col =>
            val value1 = row1.lift(      getColumnNames.indexOf(col)).getOrElse("")
            val value2 = row2.lift(other.getColumnNames.indexOf(col)).getOrElse("")
            Seq(value1, value2).filter(_.nonEmpty).distinct.mkString(";")
//            if (value1.isEmpty || value1 == value2) value2
//            else if (value2.isEmpty) value1
//            else s"$value1;$value2"
          }
        }
    }

    val      tabularRows =       getTabular.filterNot(row1 => other.getTabular.exists(row2 => row1(      getColumnNames.indexOf(key)) == row2(other.getColumnNames.indexOf(key))))
    val otherTabularRows = other.getTabular.filterNot(row2 =>       getTabular.exists(row1 => row2(other.getColumnNames.indexOf(key)) == row1(      getColumnNames.indexOf(key))))

    val mergedTable = new Table(mergedColumns, mergedRows
                                               ++
                                               tabularRows.map(
                                                 row1 => mergedColumns.map(
                                                   col => row1.lift(      getColumnNames.indexOf(col)).getOrElse("")))
                                               ++
                                               otherTabularRows.map(
                                                 row2 => mergedColumns.map(
                                                   col => row2.lift(other.getColumnNames.indexOf(col)).getOrElse(""))))
    Option(mergedTable)
  }
}

object Table {
  // 1.2
  def apply(s: String): Table =
    new Table(s.split("\n").toList.head.split(",").toList,
              s.split("\n").toList.tail.map(_.split(",", -1).toList))
}

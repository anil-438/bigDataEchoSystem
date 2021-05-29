import scala.collection.mutable

object override_veriable{
  def method01(val01:String,Val02:String):(String,String)={
    (val01,Val02)
  }

  def method02(val01:Map[String,String],Val02:List[String]):(Map[String,String],List[String])={
    (val01,Val02)
  }

  def main(args: Array[String]): Unit = {
    println( method01("Hi ","Anil"))
    val var03 = Map("a"->"value01","b"->"value02")
    var var04 = List("a","b")
    println(method02(var03,var04))
  }

}

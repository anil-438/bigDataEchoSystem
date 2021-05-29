object loops {
  //For
  def main(args: Array[String]): Unit= {
    val arr=Array(1,2,3,4,5,6,7,8,9,0)
    val arr1=Array(1,2,3,4,5,6,7,8,9,0)
    //while
    var argiteretor = arr.iterator
    while (argiteretor.hasNext){
        println(argiteretor.next())
    }
    //Do-while
    argiteretor = arr1.iterator
    do {
      println(argiteretor.next())
     } while (argiteretor.hasNext)
     //For
    for(i <- arr if i < 8  if i > 4){
      println(s"The i value is $i")
    }
    val yield_var = for(i <- arr if i < 8  if i > 4) yield i
    println(yield_var.mkString("|"))
  }

}

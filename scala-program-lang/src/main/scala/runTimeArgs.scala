
object runTimeArgs {
  def main(args: Array[String]): Unit = {
    var var2 =""
    var var3 =""
    var argIterator  = args.iterator
    while (argIterator.hasNext){
      argIterator.next() match {
        case "-var1" => var3 =argIterator.next()
        case "-var2" => var2 =argIterator.next()
        case _ => println("unknown arg")
      }
    }

  }


}

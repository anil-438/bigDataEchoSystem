object innerClass {
  class outer(val var1:String,var var2:Int){

    def outer_meth: Unit ={
      println(var2)
    }
    new inner("a",2).inner_meth//acess inner class in outer class
    class inner(val var3:String,var var4:Int){

      def inner_meth: Unit ={
        println(var2)
      }
      //  new outer("o",5).outer_meth//acess outer class in inner class // will through stack over flow error(it's loop)
    }
  }

  def main(args: Array[String]): Unit = {
    new outer("o",1)  // call outer class from main method
    val outer_var = new outer("",1)
    (new outer_var.inner("",1)).inner_meth // call inner class method from main method

  }

}

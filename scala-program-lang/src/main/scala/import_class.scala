import innerClass.outer


object import_class {
  def main(args: Array[String]): Unit = {
    val outer_var = new outer("",1)
    (new outer_var.inner("",1)).inner_meth // call another page inner class method from main method

  }

}

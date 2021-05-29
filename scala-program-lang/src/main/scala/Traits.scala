trait Printable{
  def print()         // Abstract method
}

trait Showable{
  def show(){         // Non-abstract method
    println("This is show method")
  }
}

class A5 extends Printable with  Showable{
  def print(){
    println("This is print method")
  }
}

object Traits {
  def main(args:Array[String]){
    var a = new A5()
    a.print()
    a.show()
  }
}
object abstractClass {

  abstract class PrintA4{
    def printA4()
  }

  class A6 extends PrintA4 {
    def print(){
      println("print sheet")
    }
    def printA4(){                              // Abstract class printA4
      println("Print A4 Sheet")
    }
  }

  def main(args: Array[String]): Unit = {
    (new A6).printA4()
  }

}

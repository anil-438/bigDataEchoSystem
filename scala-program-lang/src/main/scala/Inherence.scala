class Bank{
  def getRateOfInterest()={
    0
  }
}
class SBI extends Bank{
  override def getRateOfInterest()={
    8
  }
}
class ICICI extends Bank{
  override def getRateOfInterest()={
    7
  }
}

class AXIS extends Bank{
  override def getRateOfInterest()={
    9
  }
}
object Inherence{
  def main(args:Array[String]){
    var s=new SBI();
    var i=new ICICI();
    var a=new AXIS();
    println("SBI Rate of Interest: "+s.getRateOfInterest());
    println("ICICI Rate of Interest: "+i.getRateOfInterest());
    println("AXIS Rate of Interest: "+a.getRateOfInterest());
  }
}

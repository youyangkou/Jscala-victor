package com.victor.sc

case class Student(name: String, age: Int, school: String) {

  override def hashCode(): Int = {
    Utils.hashCode(name, age, school)
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Student =>
        this.name == that.name &&
          this.age == that.age &&
          this.school == that.school &&
      case _ => false
    }
  }

}

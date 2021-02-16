package samples

import com.cub.entity.MiddleStudent
import org.junit.Test

/**
  * Description：Lombok测试<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月01日  
  *
  * @author
  * @version : 1.0
  */
class TestEntity {
  @Test
  def testEntiry() = {
    val instance = new MiddleStudent("张无忌", 56.48)
    println(instance)
  }
}

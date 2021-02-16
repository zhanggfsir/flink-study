package samples

import org.junit.Test

/**
  * Description：共通测试<br/>
  * Copyright (c) ，2020 ， Jansonxu <br/>
  * This program is protected by copyright laws. <br/>
  * Date： 2020年03月05日  
  *
  * @author 徐文波
  * @version : 1.0
  */
class TestCommon {
  @Test
  def testTmpDir = {
    val value = System.getProperty("java.io.tmpdir")
    println(value)
  }
}

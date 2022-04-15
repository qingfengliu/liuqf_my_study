package test_env.framework.common

import test_env.framework.util.EnvUtil

trait TDao {
  def readFile(pathL:String)={
    EnvUtil.take.textFile(pathL)
  }
}

import pg8000.native
import src._env as _env

conn = pg8000.native.Connection(_env.user,password = _env.password,port =_env.port, database = _env.database,host = _env.host)

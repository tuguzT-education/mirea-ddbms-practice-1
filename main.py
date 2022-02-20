from cassandra.cluster import Cluster, Session
import typing as t


keyspace: t.Final[str] = 'TestKeyspace'.lower()


# noinspection PyShadowingNames
def create_session() -> Session:
    cluster = Cluster()
    session = cluster.connect(keyspace)
    return session


# noinspection PyShadowingNames
def get_users(session: Session) -> list:
    result = session.execute("SELECT * FROM users")
    return [x for x in result]


# noinspection PyShadowingNames
def execute_queries(session: Session):
    session.execute("TRUNCATE TABLE users")

    result = get_users(session)
    print(result)

    session.execute("INSERT INTO users (id, name, login, group) VALUES (1, 'User', 'setevoy', 'wheel')")
    result = get_users(session)[0]
    print(result)

    result = get_users(session)[0]
    print(result.login, result.name)

    result = get_users(session)
    for x in result:
        print(x.id)

    name, login, group = 'newuser', 'newlogin', 'newgroup'
    session.execute('INSERT INTO users (id, name, login, group) VALUES (2, %s, %s, %s)', (name, login, group))
    result = get_users(session)
    print(result)

    _dict = {'name': 'secondname', 'login': 'secondlogin', 'group': 'secondgroup', }
    session.execute('INSERT INTO users (id, name, login, group) VALUES (2, %(name)s, %(login)s, %(group)s)', _dict)
    result = get_users(session)
    print(result)


if __name__ == '__main__':
    session: t.Final[Session] = create_session()
    execute_queries(session)

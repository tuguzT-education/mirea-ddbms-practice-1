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


# noinspection PyShadowingNames
def async_queries(session: Session):
    from cassandra import ReadTimeout

    future = session.execute_async('SELECT * FROM users')
    try:
        rows = future.result()
        for user in rows:
            print(user.name, user.group)
    except ReadTimeout:
        print('ERROR: query timed out')


# noinspection PyShadowingNames
def async_queries_callback(session: Session):
    def handle_success(rows):
        try:
            for user in rows:
                print(user.name, user.group)
        except Exception as exception:
            print(f'ERROR: {exception}')

    def handle_error(exception):
        print(f'Failed to fetch user info: {exception}')

    future = session.execute_async('SELECT * FROM users')
    future.add_callbacks(handle_success, handle_error)


if __name__ == '__main__':
    session: t.Final[Session] = create_session()
    execute_queries(session)
    async_queries_callback(session)

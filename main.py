from cassandra.cluster import Cluster, Session
import typing as t


keyspace: t.Final[str] = 'TestKeyspace'.lower()


# noinspection PyShadowingNames
def create_session() -> Session:
    cluster = Cluster()
    session = cluster.connect(keyspace)
    return session


if __name__ == '__main__':
    session: t.Final[Session] = create_session()

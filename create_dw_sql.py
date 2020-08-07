import sys, getopt
import datetime


def executar_sql(text, array=True):
    import psycopg2
    try:
        conn_pg = psycopg2.connect("user='postgres' host='localhost' password='postgres' dbname='dw'")
        conn_pg.autocommit = True
    except:
        pass
    cur_pg = conn_pg.cursor()
    lista = ''
    if text:
        text =text.replace("'NULL'", 'NULL')
        cur_pg.execute( text )
        if array:
            lista = cur_pg.fetchall()
    cur_pg.close()
    conn_pg.close()
    return lista


FIELDS_SQL = """
SELECT column_name , data_type
  FROM information_schema.columns
 WHERE table_name = 'multi_nais'
   AND table_schema = 'stage'
   AND data_type IN ('character varying',
                     'character', 
                     'text', 
                     'date',
                     'timestamp without time zone');"""


CREATE_DIM_TABLE = """
DROP TABLE IF EXISTS %(tablename)s.dim_%(dim)s;
CREATE TABLE IF NOT EXISTS %(tablename)s.dim_%(dim)s (
  sk_%(dim)s SERIAL,
  %(dim)s VARCHAR(500)
);
CREATE INDEX idx_dim_%(dim)s_lookup ON %(tablename)s.dim_%(dim)s(%(dim)s);
"""


SELECT_DIM_TABLE = """
SELECT DISTINCT COALESCE(%(dim)s, '(Não informado)') AS %(dim)s
  FROM stage.%(tablename)s;
  """


CREATE_FACT_TABLE = """
CREATE TABLE IF NOT EXISTS %(tablename)s.fat_%(tablename)s (
  id INTEGER, 
%(create)s, 
  quant BIGINT);
  """


SELECT_FACT_TABLE = """
SELECT %(tablename)s.id, 
%(select)s,
       COUNT(DISTINCT %(tablename)s.id) AS quant
  FROM stage.%(tablename)s
%(inner_join)s
 GROUP BY %(tablename)s.id, 
%(group_by)s;
"""


def create(tablename):

    fields = executar_sql(FIELDS_SQL)

    SELECT = []
    INNER_JOIN = []
    GROUP_BY = []
    CREATE = []

    create_dims = ''
    select_dims = ''

    for f in fields:
        if f[1] in ('timestamp without time zone', 'date'):
            SELECT.append("       to_char(%s, 'YYYYMMDD')::int AS sk_%s" % (f[0], f[0]))
            GROUP_BY.append("          to_char(%s, 'YYYYMMDD')::int" % f[0])
            CREATE.append("  sk_%s INTEGER" % f[0])
        else:
            temp = {'tablename': tablename, 'dim': f[0]}
            create_dims += CREATE_DIM_TABLE % temp
            select_dims += SELECT_DIM_TABLE % temp
            SELECT.append("       dim_%s.sk_%s" % (f[0], f[0]))
            INNER_JOIN.append(" INNER JOIN %s.dim_%s ON dim_%s.%s = %s.%s" % (tablename, f[0], f[0], f[0], tablename, f[0]))
            GROUP_BY.append("          dim_%s.sk_%s" % (f[0], f[0]))
            CREATE.append("  sk_%s INTEGER" % f[0])

    data = {}
    data['tablename'] = tablename
    data['select'] = ',\n'.join(SELECT)
    data['inner_join'] = '\n'.join(INNER_JOIN)
    data['group_by'] = ',\n'.join(GROUP_BY)
    data['create'] = ',\n'.join(CREATE)

    print('-- CREATE DIMS TABLES')
    print(create_dims)
    print('-- SELECT DIMS TABLES')
    print(select_dims)

    print('-- CREATE FACT TABLE')
    print(CREATE_FACT_TABLE % data)
    print('-- SELECT FACT TABLE')
    print(SELECT_FACT_TABLE % data)



def main(argv):

    tablename = None

    try:
        opts, args = getopt.getopt(argv, "t:", ["tablename=", ])

    except getopt.GetoptError:
        print('python create_dw_sql.py -t <tablename>')
        sys.exit(2)

    for opt, arg in opts:

        if opt in ("-h", "--help", '-?'):
            print('python create_dw_sql.py -t <tablename>')
            sys.exit()

        elif opt in ("-t", "--tablename"):
            tablename = arg

    if tablename:
        create(tablename)
        print()
    else:
        print('Example:')
        print('python create_dw_sql.py -f <file>')


if __name__ == "__main__":
    main(sys.argv[1:])

import pandas as pd
import sqlite3

def criar_conexao(db_file):    
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(f"Conexão estabelecida com o banco de dados: {db_file}")
    except sqlite3.Error as e:
        print(f"Erro ao conectar ao banco de dados: {e}")
    return conn

def criar_tabelas(conn):  
    cursor = conn.cursor() 
    cursor.execute("""    
    CREATE TABLE IF NOT EXISTS Funcionarios (
        id_funcionario INTEGER PRIMARY KEY,
        nome_funcionario TEXT NOT NULL,
        id_cargo INTEGER,
        id_departamento INTEGER
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Cargos (
        id_cargo INTEGER PRIMARY KEY,
        nome_cargo TEXT NOT NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Departamentos (
        id_departamento INTEGER PRIMARY KEY,
        nome_departamento TEXT NOT NULL
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Dependentes (
        id_dependente INTEGER PRIMARY KEY,
        id_funcionario INTEGER,
        nome_dependente TEXT NOT NULL,
        data_nascimento TEXT NOT NULL,
        genero TEXT NOT NULL,  
        FOREIGN KEY (id_funcionario) REFERENCES Funcionarios (id_funcionario)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS HistoricoSalarios (
        id_historico INTEGER PRIMARY KEY,
        id_funcionario INTEGER,
        salario REAL NOT NULL,
        data TEXT NOT NULL,
        FOREIGN KEY (id_funcionario) REFERENCES Funcionarios (id_funcionario)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Projetos (
        id_projeto INTEGER PRIMARY KEY,
        nome_projeto TEXT NOT NULL,
        descricao TEXT,
        data_inicio TEXT,
        data_conclusao TEXT,
        id_funcionario_responsavel INTEGER,
        custo REAL,
        status TEXT CHECK(status IN ('Em Planejamento', 'Em Execução', 'Concluído', 'Cancelado')),
        FOREIGN KEY (id_funcionario_responsavel) REFERENCES Funcionarios(id_funcionario)
    )
    """)
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS Recursos (
        id_recurso INTEGER PRIMARY KEY,
        id_projeto INTEGER NOT NULL,
        descricao_recurso TEXT,
        tipo_recurso TEXT CHECK(tipo_recurso IN ('Financeiro', 'Material', 'Humano')) NOT NULL,
        quantidade_utilizada REAL,
        data_utilizacao TEXT,
        FOREIGN KEY (id_projeto) REFERENCES Projetos(id_projeto)
    )
    """) 
    conn.commit()
    print("Tabelas criadas com sucesso.")

def inserir_dados(csv_file, table_name, conn):  
    df = pd.read_csv(csv_file)
    try:
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"Dados inseridos na tabela {table_name}. Total de linhas: {len(df)}")
    except sqlite3.IntegrityError:
        print(f"Um ou mais dados já existem na tabela {table_name}. Ignorando inserção de duplicatas.")
    except Exception as e:
        print(f"Erro ao inserir dados na tabela {table_name}: {e}")

def executar_sql(conn, query):
    try: 
        df = pd.read_sql_query(query, conn)
        print(df)
    except sqlite3.Error as e:
        print(f"Erro ao executar a consulta: {e}")

# Querie 1
def media_salarios_por_departamento_projetos_concluidos(conn):
    print("\n➙  Querie 1 - Média dos salários (atual) dos funcionários responsáveis por projetos concluídos, agrupados por departamento:\n")
    
    query_1 = """
    SELECT 
        d.nome_departamento AS DEPARTAMENTO,
        AVG(hs.salario) AS MEDIA_SALARIO
    FROM 
        Funcionarios f
    JOIN 
        Projetos p ON f.id_funcionario = p.id_funcionario_responsavel
    JOIN 
        Departamentos d ON f.id_departamento = d.id_departamento
    JOIN 
        (SELECT 
             id_funcionario, salario
         FROM 
             HistoricoSalarios
         WHERE 
             (id_funcionario, data) IN (
                 SELECT id_funcionario, MAX(data) 
                 FROM HistoricoSalarios 
                 GROUP BY id_funcionario
             )
        ) hs ON f.id_funcionario = hs.id_funcionario
    WHERE 
        p.status = 'Concluído'
    GROUP BY 
        d.nome_departamento;
    """
    executar_sql(conn, query_1)

# Querie 2
def tres_recursos_mais_usados(conn):
    print("\n➙  Querie 2 - Os três recursos materiais mais usados nos projetos:\n")
    
    query_2 = """
    SELECT 
        r.descricao_recurso AS RECURSO,
        SUM(r.quantidade_utilizada) AS QUANTIDADE_TOTAL
    FROM 
        Recursos r
    WHERE 
        r.tipo_recurso = 'Material'
    GROUP BY 
        r.descricao_recurso
    ORDER BY 
        QUANTIDADE_TOTAL DESC
    LIMIT 3;
    """
    
    executar_sql(conn, query_2)

# Querie 3
def custo_por_departamento(conn):
    print("\n➙  Querie 3 - Custo total dos projetos por departamento (Concluídos):\n")
    
    query_3 = """
    SELECT 
        d.nome_departamento AS DEPARTAMENTO,
        SUM(p.custo) AS custo
    FROM 
        Departamentos d
    JOIN 
        Funcionarios f ON d.id_departamento = f.id_departamento
    JOIN 
        Projetos p ON f.id_funcionario = p.id_funcionario_responsavel
    WHERE 
        p.status = 'Concluído'
    GROUP BY 
        d.nome_departamento
    ORDER BY 
        custo DESC;
    """
    
    executar_sql(conn, query_3)

# Querie 4
def projetos_em_execucao(conn):
    print("\n➙  Querie 4 - Listar projetos que estão em execução:\n")
    
    query_4 = """
    SELECT 
        p.nome_projeto AS NOME_PROJETO,
        p.custo AS CUSTO,
        p.data_inicio AS DATA_INICIO,
        p.data_conclusao AS DATA_CONCLUSAO,
        f.nome_funcionario AS RESPONSAVEL
    FROM 
        Projetos p
    JOIN 
        Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
    WHERE 
        p.status = 'Em Execução';
    """
    
    executar_sql(conn, query_4)

# Querie 5
def dependentes_no_projeto(conn):
    print("\n➙ Querie 5 - Projeto com o maior número de dependentes associados ao responsável:\n")
    query_5 = """
SELECT 
    p.nome_projeto AS PROJETO,
    COUNT(d.id_dependente) AS TOTAL_DEPENDENTES
FROM 
    Projetos p
JOIN 
    Funcionarios f ON p.id_funcionario_responsavel = f.id_funcionario
LEFT JOIN 
    Dependentes d ON f.id_funcionario = d.id_funcionario
GROUP BY 
    p.nome_projeto
ORDER BY 
    TOTAL_DEPENDENTES DESC
LIMIT 1;

    """
    executar_sql(conn, query_5)

def main():
    conn = criar_conexao('funcionarios.db')
    
    criar_tabelas(conn)
 
    inserir_dados('csv/Cargos.csv', 'Cargos', conn)
    inserir_dados('csv/Departamentos.csv', 'Departamentos', conn)
    inserir_dados('csv/Dependentes.csv', 'Dependentes', conn)
    inserir_dados('csv/Funcionarios.csv', 'Funcionarios', conn)
    inserir_dados('csv/HistoricoSalarios.csv', 'HistoricoSalarios', conn)
    inserir_dados('csv/Projetos.csv', 'Projetos', conn)
    inserir_dados('csv/Recursos.csv', 'Recursos', conn)

    print("\n ···························· QUERIES  ····························")
    media_salarios_por_departamento_projetos_concluidos(conn)  
    tres_recursos_mais_usados(conn) 
    custo_por_departamento(conn)
    projetos_em_execucao(conn)
    dependentes_no_projeto(conn)

    conn.close()

if __name__ == "__main__":
    main()

from pyspark.sql import SparkSession

class DeltaTableMetadataManager:
    """
    Classe responsável por gerenciar metadados de tabelas Delta, como comentários de colunas e tabelas.
    """

    @staticmethod
    def add_column_comments(table_path: str, comments: dict, spark=None) -> None:
        """
        Adiciona comentários em múltiplas colunas de uma tabela Delta.

        Args:
            table_path (str): Caminho completo da tabela Delta (ex: 'catalog.schema.tabela').
            comments (dict): Dicionário com o nome da coluna como chave e o comentário como valor.
            spark (SparkSession, optional): Sessão Spark ativa. Se não for fornecida, será criada automaticamente.
        """
        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

        for col, comment in comments.items():
            comment = comment.strip().replace("'", "''")
            query = f"ALTER TABLE {table_path} ALTER COLUMN {col} COMMENT '{comment}'"
            spark.sql(query)

    @staticmethod
    def add_table_comment(table_path: str, comment: str, spark=None) -> None:
        """
        Adiciona um comentário descritivo à tabela Delta.

        Args:
            table_path (str): Caminho completo da tabela Delta (ex: 'catalog.schema.tabela').
            comment (str): Comentário descritivo para a tabela.
            spark (SparkSession, optional): Sessão Spark ativa. Se não for fornecida, será criada automaticamente.
        """
        if not isinstance(comment, str):
            return

        if spark is None:
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()

        comment = comment.strip().replace("'", "''")
        spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('comment' = '{comment}')")


class DeltaTableConstraintManager:
    """
    Classe responsável por aplicar constraints (NOT NULL, PRIMARY KEY, FOREIGN KEY) em tabelas Delta.
    """

    @staticmethod
    def set_column_not_null(table_path: str, column_name: str, spark=None) -> None:
        """
        Define a coluna como NOT NULL em uma tabela Delta.

        Args:
            table_path (str): Caminho completo da tabela Delta (ex: 'catalog.schema.tabela').
            column_name (str): Nome da coluna a ser alterada para NOT NULL.
            spark (SparkSession, optional): Sessão Spark ativa. Se não for fornecida, será criada automaticamente.
        """
        if spark is None:
            spark = SparkSession.builder.getOrCreate()
        spark.sql(f"ALTER TABLE {table_path} ALTER COLUMN {column_name} SET NOT NULL")

    @staticmethod
    def add_primary_key(table_path: str, column_name, constraint_name: str, spark=None) -> None:
        """
        Adiciona uma constraint de PRIMARY KEY, composta ou simples.

        Garante que todas as colunas envolvidas na chave primária sejam definidas como NOT NULL.
        Se a constraint já existir, a função ignora a criação para evitar falhas no notebook.

        Args:
            table_path (str): Caminho completo da tabela no formato 'catalog.schema.tabela'.
            column_name (str or list): Nome da coluna ou lista de colunas que compõem a chave primária.
            constraint_name (str): Nome da constraint de chave primária a ser criada.
            spark (SparkSession, optional): Sessão Spark ativa. Se não for fornecida, uma nova será criada.

        Returns:
            None
        """
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        if isinstance(column_name, str):
            column_names = [column_name]
        elif isinstance(column_name, list):
            column_names = column_name
        else:
            print("[ERRO] column_name deve ser uma string ou lista de strings.")
            return

        for col in column_names:
            try:
                spark.sql(f"ALTER TABLE {table_path} ALTER COLUMN {col} SET NOT NULL")
            except Exception as e:
                print(f"[WARN] Falha ao definir coluna '{col}' como NOT NULL: {e}")

        col_str = ", ".join(column_names)

        try:
            spark.sql(f"ALTER TABLE {table_path} ADD CONSTRAINT {constraint_name} PRIMARY KEY ({col_str})")
            print(f"[INFO] Constraint '{constraint_name}' adicionada com sucesso na tabela {table_path}.")
        except Exception as e:
            error_message = str(e)
            if "DELTA_CONSTRAINT_ALREADY_EXISTS" in error_message or "already exists" in error_message:
                print(f"[INFO] Constraint '{constraint_name}' já existe. Nenhuma ação necessária.")
            else:
                print(f"[ERRO] Falha ao adicionar a constraint '{constraint_name}': {e}")

    @staticmethod
    def add_foreign_key(
        table_path: str,
        column_names,
        ref_table: str,
        ref_columns,
        constraint_name: str,
        spark=None
    ) -> None:
        """
        Adiciona uma constraint de FOREIGN KEY (simples ou composta).
        Se a constraint já existir, não faz nada e segue o processo sem erro.

        Args:
            table_path (str): Caminho completo da tabela destino (ex: 'catalog.schema.tabela').
            column_names (str or list): Nome da coluna ou lista de colunas da tabela destino.
            ref_table (str): Caminho completo da tabela de referência.
            ref_columns (str or list): Nome da coluna ou lista de colunas da tabela de referência.
            constraint_name (str): Nome da constraint a ser criada.
            spark (SparkSession, optional): Sessão Spark ativa. Se não informado, cria uma nova.
        """
        if spark is None:
            spark = SparkSession.builder.getOrCreate()

        if isinstance(column_names, str):
            column_names = [column_names]
        if isinstance(ref_columns, str):
            ref_columns = [ref_columns]

        if len(column_names) != len(ref_columns):
            print("[ERRO] As listas de colunas devem ter o mesmo comprimento.")
            return

        col_dest = ", ".join(column_names)
        col_ref = ", ".join(ref_columns)

        query = f"""
            ALTER TABLE {table_path}
            ADD CONSTRAINT {constraint_name}
            FOREIGN KEY ({col_dest})
            REFERENCES {ref_table} ({col_ref})
        """

        try:
            spark.sql(query)
            print(f"[INFO] Constraint FOREIGN KEY '{constraint_name}' adicionada com sucesso na tabela {table_path}.")
        except Exception as e:
            error_message = str(e)
            if "DELTA_CONSTRAINT_ALREADY_EXISTS" in error_message or "already exists" in error_message or "SQLSTATE: 42710" in error_message:
                print(f"[INFO] Constraint FOREIGN KEY '{constraint_name}' já existe na tabela {table_path}. Pulando criação.")
            else:
                print(f"[ERRO] Falha ao adicionar a constraint FOREIGN KEY '{constraint_name}': {e}")

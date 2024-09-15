import numpy as np
import pandas as pd
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select, text
from sqlalchemy.engine import reflection
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

class SqlConnect():
    def __init__(self):
        self.engine = create_engine('User MySql Engine')
        self.metadata = MetaData()
        self.status_table_name = "update_status"
        self.create_status_table()

    def table_setting(self, df, name):
        # 테이블 이름
        self.table_name = name

        # 메타데이터 객체 생성
        metadata = MetaData()

        self.stock_data_table = Table(
            self.table_name, metadata,
            Column('date', String(20), primary_key=True),  # 날짜를 기본 키로 사용
            Column('open', Integer),                   # 시가
            Column('high', Integer),                   # 고가
            Column('low', Integer),                    # 저가
            Column('close', Integer),                  # 종가
            Column('volume', Integer)                  # 거래량
        )

        self.table_create(df, name, self.stock_data_table)

    def create_status_table(self):
        # Create a status table if it does not exist
        inspector = reflection.Inspector.from_engine(self.engine)
        if self.status_table_name not in inspector.get_table_names():
            status_table = Table(
                self.status_table_name, self.metadata,
                Column('date', String(20), primary_key=True),  # 날짜를 기본 키로 사용
                Column('all_tables_updated', Integer)          # 모든 테이블 업데이트 상태 (1 or 0)
            )
            with self.engine.connect() as connection:
                status_table.create(connection)
            print(f"Table '{self.status_table_name}' created successfully.")
        else:
            print(f"Table '{self.status_table_name}' already exists.")

    # 테이블 존재 여부 확인
    def table_create(self, df, table_name, stock_data_table):
        inspector = reflection.Inspector.from_engine(self.engine)
        # 테이블이 존재하지 않으면 생성
        if table_name not in inspector.get_table_names():
            with self.engine.connect() as connection:
                stock_data_table.create(connection)
            print(f"Table '{table_name}' created successfully.")
        else:
            print(f"Table '{table_name}' already exists.")

        # 데이터프레임을 SQL 테이블로 저장
        df.to_sql(name=table_name, con=self.engine, if_exists='append', index=False)
        print(f"DataFrame {table_name} successfully saved to SQL database.")

    # SQL 테이블에서 데이터 가져오기
    def fetch_data(self, table_name):
        with self.engine.connect() as connection:
            # SQLAlchemy's select statement to fetch data
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)

            query = select(table)
            result = connection.execute(query)

            # Convert result to a Pandas DataFrame
            df = pd.DataFrame(result.fetchall(), columns=result.keys())

        print(f"Data from table '{table_name}' fetched successfully.")
        return df

    def get_all_table_names(self):
        inspector = reflection.Inspector.from_engine(self.engine)
        table_names = inspector.get_table_names()
        print(f"Existing table names: {table_names}")
        return table_names

    def update_status_table(self):
        # Fetch today's date
        today = datetime.now().strftime('%Y-%m-%d')

        # Fetch all table names excluding the status table
        inspector = reflection.Inspector.from_engine(self.engine)
        table_names = [table for table in inspector.get_table_names() if table != self.status_table_name]

        # If there are no other tables, set status to 0 and return False
        if not table_names:
            all_updated = 0
            self.update_status_in_db(today, all_updated)
            print("No tables other than the update_status table exist.")
            return False

        all_updated = 1  # Assume all tables are updated

        with self.engine.connect() as connection:
            for table_name in table_names:
                # Check if today's date is present in the table
                query = f"SELECT COUNT(*) FROM `{table_name}` WHERE date = '{today}'"  # Use backticks to enclose table names
                result = connection.execute(text(query)).scalar()

                if result == 0:  # If no records for today's date
                    all_updated = 0
                    print(f"Table '{table_name}' does not have today's data.")
                    break

        # Update the status table with the appropriate value
        self.update_status_in_db(today, all_updated)

        # Return True if all tables are updated, otherwise False
        return all_updated == 1

    def update_status_in_db(self, date, all_updated):
        # Insert or update status in the status table using text()
        with self.engine.connect() as connection:
            status_query = text(f"""
                INSERT INTO {self.status_table_name} (date, all_tables_updated) 
                VALUES ('{date}', {all_updated}) 
            """)
            connection.execute(status_query)
            print(f"Status table updated for date {date} with value {all_updated}.")

    def add_table_column(self, table_name, column_name, column_type):
        # Query to check if the column exists
        check_column_query = """
        SELECT COUNT(*)
        FROM information_schema.COLUMNS
        WHERE TABLE_NAME = :table_name 
        AND COLUMN_NAME = :column_name
        """

        # Execute the query within a connection
        with self.engine.connect() as connection:
            # Fetch the result using a parameterized query
            result = connection.execute(text(check_column_query),
                                        {'table_name': table_name, 'column_name': column_name}).fetchone()

            # If the column doesn't exist, add it
            if result[0] == 0:
                alter_query = f"""
                ALTER TABLE `{table_name}` 
                ADD COLUMN `{column_name}` {column_type}
                """
                connection.execute(text(alter_query))
                print(f"Column `{column_name}` added to table `{table_name}`")
            else:
                print(f"Column `{column_name}` already exists in table `{table_name}`")

    import pandas as pd

    def batch_update_table_from_series(self, table_name, column_name, series):
        """Pandas Series의 값만 배열로 변환하여 대량으로 데이터베이스에 업데이트합니다."""

        # 입력이 Pandas Series인지 확인
        if not isinstance(series, pd.Series):
            raise ValueError("입력은 Pandas Series여야 합니다.")

        # Series의 value만 배열로 변환하면서 NaN 값을 None으로 변환
        values = [None if pd.isna(value) else round(value, 3) for value in series.values]

        # 디버그: 변환된 value 배열 확인
        print(f"Values array before batch update for column `{column_name}`:")
        print(values[:5])  # 처음 5개의 항목을 출력

        with self.engine.connect() as connection:
            # 트랜잭션 시작
            trans = connection.begin()
            try:
                # 원시 SQL 쿼리: `value`만 업데이트 (배치 처리)
                update_query = text(f"""
                    INSERT INTO `{table_name}` (`date`, `{column_name}`)
                    VALUES (:date, :value)
                    ON DUPLICATE KEY UPDATE `{column_name}` = VALUES(`{column_name}`)
                """)

                # 여러 행을 execute()로 한 번에 처리
                update_data = [{'date': date, 'value': value} for date, value in zip(series.index, values)]

                # `execute()`에 여러 행을 전달
                connection.execute(update_query, update_data)

                # 트랜잭션 커밋
                trans.commit()
                print(f"Batch update successful for column `{column_name}`.")
            except Exception as e:
                # 에러 발생 시 롤백
                trans.rollback()
                print(f"대량 업데이트 중 예외 발생 for column `{column_name}`: {e}")

















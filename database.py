import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

# Carga variables del archivo .env
load_dotenv()

# Lee la cadena de conexión de la BD
DATABASE_URL = os.getenv("DATABASE_URL")

# Crea el engine de SQLAlchemy
engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)

# Crea la fábrica de sesiones
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base para los modelos
Base = declarative_base()

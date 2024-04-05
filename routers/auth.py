import uuid
from os import environ
from datetime import datetime, timedelta, timezone
from typing import Annotated
from dotenv import load_dotenv
from fastapi import Depends, HTTPException, status, APIRouter
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from sqlalchemy import text
from models.auth import LoginDetails, User, LoginRes, RegisterRes
from db import DB


router = APIRouter(
    prefix='/api/auth',
    tags=['Authorization']
)

# Salasanan hashays
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# PW bearer. TokenURL on sama kuin login openapi funktion endpoint
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/auth/login/openapi")

# Tokenin luontiin ja käsittelyyn liittyvä tieto ja funktio
# Secret key on meidän randomisti generoitu 32 merkin hex, joka on envissä.
load_dotenv(dotenv_path=".env")

SECRET_KEY = environ.get("SECRET")
ALGORITHM = "HS256"


# Luodaan access token, jota käytetään autentisontiin. Tokenille voidaan määritellä vanhentumisaika
def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


# Salasanan häshäys ja varmistus
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password):
    return pwd_context.hash(password)


# Autentisoi käyttäjän login tiedot ja palauttaa User data classin, mikäli käyttäjätunnus ja salasana ovat oikein
def authenticate_user(db: DB, username: str, password: str):
    _query = text("SELECT * FROM auth_users WHERE username = :username")
    user = db.execute(_query, {"username": username}).mappings().first()

    if not user:
        return False
    if not verify_password(password, user["password"]):
        return False

    return User(user_id=user['user_id'], username=user['username'], role_id=user['auth_role_id'])


# Haetaan kirjautunut käyttäjä
# token: Annotated[str, Depends(oauth2_scheme)] määrittää, että headerissa halutaan token bearer
# Jonka avulla funktio saa haltuunsa jwt subin, eli access_jti, jonka avulla taasen haetaan käyttäjä.
def get_current_user(db: DB, token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        access_jti: str = payload.get("sub")
        if access_jti is None:
            print("no access_jti")
            raise credentials_exception

    except JWTError:
        print("JWT error")
        raise credentials_exception

    user = get_user_by_access_token_identifier(db, access_jti)

    if not user:
        print("no user")
        raise credentials_exception

    return user


# Haetaan käyttäjä access_jti:n perusteella, jota käytetään tokenin subina
def get_user_by_access_token_identifier(db: DB, sub):
    user = db.execute(
        text("SELECT * FROM auth_users WHERE access_jti = :sub"),
        {"sub": sub}
    ).mappings().first()

    if not user:
        return False

    return User(user_id=user['user_id'], username=user['username'], role_id=user['auth_role_id'])


# Hae logged in user, vaatii tokenin
@router.get("/user")
async def get_logged_in_user(db: DB, token: Annotated[str, Depends(oauth2_scheme)]) -> User:
    return get_current_user(db, token)


# Luo tietokantaan uusi käyttäjä
@router.post("/register")
async def register_user(db: DB, req: LoginDetails) -> RegisterRes:
    _insert = text("INSERT INTO auth_users(username, password, created_at, auth_role_id) "
                   "VALUES(:username, :password, NOW(), :role_id)")

    # Häshätään salasana
    hashed_pw = get_password_hash(req.password)

    # Löytyykö user role tietokannasta
    role_id = db.execute(text("SELECT role_id FROM auth_roles WHERE role_name = 'user'")).mappings().first()
    if role_id is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Role not found",
        )
    role_id = role_id['role_id']

    # Onko käyttäjänimi jo käytössä
    user_in_db = db.execute(
        text("SELECT username FROM auth_users WHERE username = :username"),
        {"username": req.username}
    ).mappings().first()

    if user_in_db is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="User already exists",
        )

    # Voidaan suorittaa insert ja commitoida se
    db.execute(
        _insert,
        {
            "username": req.username,
            "password": hashed_pw,
            "role_id": role_id
        }
    )
    db.commit()

    return RegisterRes(username=req.username, role_id=role_id)


# Login openapin docsin kaavakkeella
@router.post("/login/openapi")
async def login_openapi(
        db: DB,
        form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
) -> LoginRes:
    user = authenticate_user(db, form_data.username, form_data.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Luodaan access_token sub tietokantaan
    access_token_sub = str(uuid.uuid4())

    db.execute(
        text("UPDATE auth_users SET access_jti = :sub WHERE auth_users.user_id = :id"),
        {
            "sub": access_token_sub,
            "id": user.user_id
        }
    )
    db.commit()

    access_token = create_access_token(
        data={"sub": access_token_sub}, expires_delta=timedelta(minutes=15)
    )

    return LoginRes(access_token=access_token, user=user)


# Normaali login request
@router.post("/login")
async def login(
        db: DB,
        req: LoginDetails
) -> LoginRes:
    user = authenticate_user(db, req.username, req.password)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Luodaan access_token sub tietokantaan
    access_token_sub = str(uuid.uuid4())

    db.execute(
        text("UPDATE auth_users SET access_jti = :sub WHERE auth_users.user_id = :id"),
        {
            "sub": access_token_sub,
            "id": user.user_id
        }
    )
    db.commit()

    access_token = create_access_token(
        data={"sub": access_token_sub}, expires_delta=timedelta(days=7)
    )

    return LoginRes(access_token=access_token, user=user)


# Poistetaan jti tietokannasta uloskirjautuessa. Vaatii tokenin
@router.post("/logout")
async def logout(db: DB, token: Annotated[str, Depends(oauth2_scheme)]):
    user = get_current_user(db, token)

    db.execute(
        text("UPDATE auth_users SET access_jti = NULL WHERE auth_users.user_id = :id"),
        {"id": user.user_id}
    )
    db.commit()

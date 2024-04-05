from pydantic import BaseModel


class LoginDetails(BaseModel):
    username: str
    password: str


class User(BaseModel):
    user_id: int
    username: str
    role_id: int


class LoginRes(BaseModel):
    access_token: str
    user: User


class RegisterRes(BaseModel):
    username: str
    role_id: int

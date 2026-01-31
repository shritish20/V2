"""
FastAPI Dependencies
"""
from fastapi import Request, Depends
from typing import Generator

def get_db_writer(request: Request):
    return request.app.state.db_writer

def get_telegram(request: Request):
    return request.app.state.telegram

def get_circuit_breaker(request: Request):
    return request.app.state.circuit_breaker

def get_upstox_client(request: Request):
    return request.app.state.upstox_client

def get_greeks_manager(request: Request):
    return request.app.state.greeks_manager

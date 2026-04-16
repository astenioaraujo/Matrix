from io import BytesIO

from flask import Blueprint, redirect, url_for, session, request, send_file
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Border, Side, Alignment
from openpyxl.formatting.rule import ColorScaleRule
from openpyxl.utils import get_column_letter

from services.matricial_service import obter_dados_matricial

relatorios_bp = Blueprint("relatorios", __name__)
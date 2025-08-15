from .storage import Storage

from datetime import datetime
import os
import json
from pathlib import Path
import shutil
import glob
from typing import List, Optional, Dict, Union


class TimeSeriesStorage(Storage):
    """
    Classe que lida com o armazenamento em série temporal com capacidade de pesquisa
    """

    def __init__(
        self,
        at: str,
        max_size: int = 5,
        path_format: Optional[str] = None,
        filename_pattern: Optional[str] = None
    ):
        """
        Constrói um objeto que armazenará e pesquisará arquivos em série
        temporal

        Args:
            at (str): Caminho base para armazenamento
            max_size (int): Número máximo de arquivos antes da rotação
            path_format (str): Formato do caminho com placeholders
            filename_pattern (str, optional): Padrão do nome do arquivo. Se
            None, retorna apenas o diretório.
        """
        super().__init__(at)
        self.max_size = max_size
        self.path_format = path_format or '{year}{month}{day}/{hour}{minute}/{product}'
        self.filename_pattern = filename_pattern

    def health_check(self, product: str) -> None:
        """
        Verifies and cleans up date entries in the date.json file for a
        specific product.
        Removes entries that don't correspond to actual files in the storage.

        Args:
            product: Name of the product to check
        """
        date_file = Path(f'{self.path}/dates/date_{product}.json')

        if not date_file.exists():
            return

        # Load existing dates
        with open(date_file, 'r') as file:
            data = json.load(file)

        # Keep track of valid dates
        valid_dates = []
        removed_count = 0

        for date_str in data["dates"]:
            try:
                date = datetime.strptime(date_str, "%Y-%m-%dT%H:%MZ")
                full_path = self._generate_full_path(product, date)

                if os.path.exists(full_path):
                    valid_dates.append(date_str)
                else:
                    removed_count += 1
            except ValueError:
                # Skip malformed date strings
                removed_count += 1
                continue

        # Only update the file if changes were made
        if removed_count > 0:
            data["dates"] = valid_dates
            with open(date_file, 'w') as file:
                json.dump(data, file)

            print(
                f"Health check removed {removed_count} invalid entries for" +
                f"product {product}"
            )

        return removed_count

    def _generate_placeholders(
        self,
        product: str,
        date: datetime
    ) -> Dict[str, str]:
        """
        Gera os valores dos placeholders para uma data e produto
        específicos
        """
        rounded_minute = int(f'{date.minute:02.0f}'[0] + '0')
        date = date.replace(minute=rounded_minute)

        return {
            'year': date.strftime('%Y'),
            'month': date.strftime('%m'),
            'day': date.strftime('%d'),
            'hour': date.strftime('%H'),
            'minute': date.strftime('%M'),
            'day_of_year': date.strftime('%j'),
            'product': product
        }

    def _generate_full_path(self, product: str, date: datetime) -> str:
        """Gera o caminho completo baseado nos formatos configurados"""
        placeholders = self._generate_placeholders(product, date)

        # Gera as partes do caminho
        path_parts = self.path_format.format(**placeholders)
        full_path = os.path.join(self.path, path_parts)

        # Adiciona o nome do arquivo se o pattern foi especificado
        if self.filename_pattern is not None:
            filename = self.filename_pattern.format(**placeholders)
            full_path = os.path.join(full_path, filename)

        return full_path

    def dispose(self) -> None:
        """
        Removes all empty directories within the storage path (self.path).
        Walks through the directory tree bottom-up and removes directories
        that are empty.
        """
        for root, dirs, files in os.walk(self.path, topdown=False):
            for dir_name in dirs:
                dir_path = os.path.join(root, dir_name)
                try:
                    # Try to remove the directory if it's empty
                    os.rmdir(dir_path)
                except OSError:
                    # Directory not empty or other error - skip it
                    pass

        # Also try to remove the base directory if it's now empty
        try:
            os.rmdir(self.path)
        except OSError:
            # Base directory not empty or other error - skip it
            pass

    def _remove_oldest(self, product: str, of: List[str]):
        """Remove os arquivos mais antigos até que a quantidade esteja dentro
        do limite max_size"""
        while len(of) > self.max_size:
            to_remove = of.pop(0)
            removed_date = datetime.strptime(to_remove, "%Y-%m-%dT%H:%MZ")
            full_path = self._generate_full_path(product, removed_date)

            if os.path.exists(full_path):
                if os.path.isfile(full_path):
                    os.remove(full_path)
                else:
                    shutil.rmtree(full_path)

    def new(
        self,
        product: str,
        date: datetime,
        use_dates_folder: bool = True
    ) -> str:
        """
        Cria um novo caminho para armazenamento baseado na data

        Args:
            product: Nome do produto
            date: Data do arquivo
            use_dates_folder: Se True, mantém registro das datas no arquivo
            JSON

        Returns:
            Caminho completo para o novo arquivo/diretório
        """
        if use_dates_folder:
            Path(f'{self.path}/dates').mkdir(exist_ok=True)
            data = {"dates": []}
            date_file = Path(f'{self.path}/dates/date_{product}.json')

            if date_file.exists():
                with open(date_file, 'r+') as file:
                    data = json.load(file)

            rounded_minute = int(f'{date.minute:02.0f}'[0] + '0')
            date = date.replace(minute=rounded_minute)
            date_str = date.strftime("%Y-%m-%dT%H:%MZ")

            if date_str not in data["dates"]:
                data["dates"].append(date_str)

                if (
                    self.max_size != 'unlimited' and
                    len(data["dates"]) > self.max_size
                ):
                    self._remove_oldest(product, of=data["dates"])

                with open(date_file, 'w') as file:
                    json.dump(data, file)

        full_path = self._generate_full_path(product, date)
        os.makedirs(os.path.dirname(full_path), exist_ok=True)
        return full_path

    def find_by_date(
        self,
        product: str,
        date: datetime,
        exact_match: bool = True
    ) -> Union[str, List[str], None]:
        """
        Encontra arquivos/diretórios para uma data específica

        Args:
            product: Nome do produto
            date: Data para pesquisa
            exact_match: Se True, busca apenas a data exata. Se False, busca
            todos os arquivos próximos.

        Returns:
            - Se exact_match=True: retorna o caminho completo ou None se não
            encontrado
            - Se exact_match=False: retorna lista de caminhos encontrados (
            pode ser vazia)
        """
        if exact_match:
            full_path = self._generate_full_path(product, date)
            return full_path if os.path.exists(full_path) else None
        else:
            # Gera o padrão de busca substituindo os placeholders
            placeholders = self._generate_placeholders(product, date)
            path_pattern = self.path_format.format(**placeholders)
            path_pattern = path_pattern.replace('{', '*').replace('}', '*')

            if self.filename_pattern is not None:
                file_pattern = self.filename_pattern.format(**placeholders)
                file_pattern = file_pattern.replace('{', '*').replace('}', '*')
                search_pattern = os.path.join(
                    self.path, path_pattern, file_pattern)
            else:
                search_pattern = os.path.join(self.path, path_pattern)

            result = glob.glob(search_pattern)
            if result != []:
                return result

            result = glob.glob(search_pattern+'.*')
            if result != []:
                return result
            
            return None

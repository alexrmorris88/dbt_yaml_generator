"""
Description generator module using spacy to analyze column data and generate descriptions.
"""

import re
import spacy
import logging
from typing import List, Dict, Any, Optional
from collections import Counter

logger = logging.getLogger(__name__)

# Load spacy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    logger.warning("Spacy model not found. Downloading 'en_core_web_sm'...")
    from spacy.cli import download
    download("en_core_web_sm")
    nlp = spacy.load("en_core_web_sm")

class DescriptionGenerator:
    """Generates descriptions for tables and columns using spacy and data analysis"""
    
    def __init__(self):
        """Initialize the description generator"""
        self.nlp = nlp
    
    def _clean_column_name(self, column_name: str) -> str:
        """Convert column name to readable format"""
        # Replace underscores with spaces
        name = column_name.replace('_', ' ').lower()
        # Handle common abbreviations
        name = re.sub(r'\badt\b', 'audit', name)
        name = re.sub(r'\banb\b', 'account number', name)
        name = re.sub(r'\bsk\b', 'surrogate key', name)
        name = re.sub(r'\bpk\b', 'primary key', name)
        name = re.sub(r'\bfk\b', 'foreign key', name)
        name = re.sub(r'\bid\b', 'identifier', name)
        name = re.sub(r'\bscd\b', 'slowly changing dimension', name)
        name = re.sub(r'\bdob\b', 'date of birth', name)
        name = re.sub(r'\bssn\b', 'social security number', name)
        name = re.sub(r'\bnum\b', 'number', name)
        name = re.sub(r'\bamt\b', 'amount', name)
        name = re.sub(r'\bqty\b', 'quantity', name)
        name = re.sub(r'\baddr\b', 'address', name)
        name = re.sub(r'\btel\b', 'telephone', name)
        # Capitalize first letter
        if name:
            name = name[0].upper() + name[1:]
        return name
    
    def _analyze_sample_data(self, data: List[Any]) -> Dict[str, Any]:
        """Analyze sample data to extract patterns and common values"""
        analysis = {
            'type': None,
            'common_values': None,
            'patterns': [],
            'entity_types': [],
            'stats': {}
        }
        
        if not data or len(data) == 0:
            return analysis
        
        # Analyze data types
        types = set()
        numbers = []
        for item in data:
            if item is None:
                continue
            item_type = type(item).__name__
            types.add(item_type)
            if item_type in ('int', 'float', 'Decimal'):
                try:
                    numbers.append(float(item))
                except (ValueError, TypeError):
                    pass
        
        # Set the predominant type
        if len(types) == 1:
            analysis['type'] = list(types)[0]
        elif len(types) > 1:
            type_counts = Counter([type(item).__name__ for item in data if item is not None])
            analysis['type'] = type_counts.most_common(1)[0][0]
        
        # Calculate stats for numeric data
        if numbers:
            analysis['stats'] = {
                'min': min(numbers),
                'max': max(numbers),
                'avg': sum(numbers) / len(numbers)
            }
        
        # Extract common values for categorical data
        if analysis['type'] in ('str', 'string'):
            value_counts = Counter([str(item) for item in data if item is not None])
            common_values = value_counts.most_common(5)
            if common_values:
                analysis['common_values'] = common_values
            
            # NLP analysis for text data
            sample_text = " ".join([str(item) for item in data if item is not None])
            if len(sample_text) > 0:
                doc = self.nlp(sample_text[:10000])  # Limit to prevent processing too much text
                
                # Extract entities
                entities = Counter([ent.label_ for ent in doc.ents])
                if entities:
                    analysis['entity_types'] = entities.most_common(3)
        
        return analysis
    
    def generate_column_description(self, column_name: str, data_type: str, sample_data: List[Any]) -> str:
        """Generate a description for a column based on its name and sample data"""
        # Clean column name to make it readable
        clean_name = self._clean_column_name(column_name)
        
        # Analyze sample data
        analysis = self._analyze_sample_data(sample_data)
        
        # Start with basic description based on column name
        description = f"{clean_name}"
        
        # Handle special column types based on name patterns
        if re.search(r'id$|^id|_id$', column_name.lower()):
            if re.search(r'^pk_|^primary_', column_name.lower()):
                description = f"Primary identifier for {clean_name}"
            elif re.search(r'^fk_|^foreign_', column_name.lower()):
                description = f"Foreign key reference to {clean_name}"
            else:
                description = f"Identifier for {clean_name}"
        
        elif 'date' in column_name.lower() or re.search(r'_dt$|_date$', column_name.lower()):
            if 'create' in column_name.lower() or 'insert' in column_name.lower():
                description = f"Date when the record was created"
            elif 'update' in column_name.lower() or 'modify' in column_name.lower():
                description = f"Date when the record was last updated"
            elif 'valid_from' in column_name.lower():
                description = f"Date from which this record is valid"
            elif 'valid_to' in column_name.lower():
                description = f"Date until which this record is valid"
            elif 'birth' in column_name.lower():
                description = f"Date of birth"
            else:
                description = f"Date associated with {clean_name}"
        
        elif 'amount' in column_name.lower() or re.search(r'_amt$|_amount$', column_name.lower()):
            description = f"Monetary amount for {clean_name}"
            
            # Add range information if available
            if 'stats' in analysis and analysis['stats']:
                min_val = analysis['stats'].get('min')
                max_val = analysis['stats'].get('max')
                if min_val is not None and max_val is not None:
                    description += f" (ranges from {min_val:.2f} to {max_val:.2f})"
        
        elif 'count' in column_name.lower() or 'qty' in column_name.lower() or 'quantity' in column_name.lower():
            description = f"Count or quantity of {clean_name}"
        
        elif re.search(r'is_|_flag$|_flg$', column_name.lower()):
            description = f"Flag indicating {clean_name}"
            if analysis['common_values']:
                values = [v[0] for v in analysis['common_values']]
                description += f" (possible values: {', '.join(str(v) for v in values)})"
        
        # Special handling for audit and SCD columns
        elif column_name.lower() == 'adt_load_date':
            description = 'Audit load date used for record identification across the data pipeline'
        elif column_name.lower() == 'adt_file_source':
            description = 'Audit file source used for record identification across the data pipeline'
        elif column_name.lower() == 'adt_hash_key':
            description = 'Hash key used for record identification across the data pipeline'
        elif column_name.lower() == 'dbt_scd_id':
            description = 'SCD type 2 identifier, used for tracking different versions of a each record'
        elif column_name.lower() == 'dbt_updated_at':
            description = 'SCD type 2 updated timestamp, indicating when the record was last modified'
        elif column_name.lower() == 'dbt_valid_from':
            description = 'SCD type 2 validity start timestamp, indicating since when the record was valid'
        elif column_name.lower() == 'dbt_valid_to':
            description = 'SCD type 2 validity end timestamp, indicating since when the record was valid'
        elif column_name.lower() == 'record_sk':
            description = 'Unique key identifier for each record'
            
        # Add data type information for other columns
        elif data_type:
            # Extract simple type name
            type_name = data_type.split('(')[0].lower()
            
            if 'varchar' in type_name or 'char' in type_name or 'string' in type_name or 'text' in type_name:
                description = f"Text field containing {clean_name}"
                
                # Add common values if available
                if analysis['common_values'] and len(analysis['common_values']) <= 3:
                    values = [v[0] for v in analysis['common_values']]
                    description += f" (e.g., {', '.join(str(v) for v in values)})"
            
            elif 'int' in type_name or 'number' in type_name or 'decimal' in type_name or 'float' in type_name:
                description = f"Numeric value representing {clean_name}"
            
            elif 'date' in type_name or 'time' in type_name:
                description = f"Date/time value for {clean_name}"
            
            elif 'bool' in type_name:
                description = f"Boolean flag indicating {clean_name}"
        
        return description
    
    def generate_table_description(self, table_name: str, columns: List[Dict[str, Any]]) -> str:
        """Generate a description for a table based on its name and columns"""
        # Clean table name
        clean_name = self._clean_column_name(table_name)
        
        # Start with basic description
        description = f"Contains information related to {clean_name}"
        
        # Look for key column types to enhance description
        has_id = False
        has_name = False
        has_date = False
        has_amount = False
        has_status = False
        
        important_columns = []
        
        for column in columns:
            col_name = column['name'].lower()
            
            # Check for identifier columns
            if re.search(r'id$|^id|_id$|key$|^key|_key$', col_name) and 'dbt_' not in col_name and 'adt_' not in col_name:
                has_id = True
                if not col_name == 'id':  # Skip generic 'id'
                    ref_entity = col_name.replace('_id', '').replace('id_', '')
                    if ref_entity and len(ref_entity) > 2:  # Avoid short abbreviations
                        important_columns.append(ref_entity)
            
            # Check for name columns
            elif 'name' in col_name:
                has_name = True
                entity = col_name.replace('_name', '').replace('name_', '')
                if entity and len(entity) > 2:  # Avoid short abbreviations
                    important_columns.append(entity)
            
            # Check for date columns
            elif 'date' in col_name and 'dbt_' not in col_name and 'adt_' not in col_name:
                has_date = True
                if not col_name == 'date':  # Skip generic 'date'
                    date_type = col_name.replace('_date', '').replace('date_', '')
                    if date_type and len(date_type) > 2:  # Avoid short abbreviations
                        important_columns.append(date_type)
            
            # Check for amount columns
            elif 'amount' in col_name or '_amt' in col_name:
                has_amount = True
                amount_type = col_name.replace('_amount', '').replace('amount_', '').replace('_amt', '').replace('amt_', '')
                if amount_type and len(amount_type) > 2:  # Avoid short abbreviations
                    important_columns.append(amount_type)
            
            # Check for status columns
            elif 'status' in col_name or 'state' in col_name:
                has_status = True
        
        # Build a more detailed description
        if important_columns:
            # Get unique, non-empty values
            entities = list(set([c for c in important_columns if c]))
            
            if entities:
                # Convert to readable format
                readable_entities = [self._clean_column_name(e) for e in entities]
                unique_entities = list(set(readable_entities))
                
                if len(unique_entities) <= 3:
                    description = f"Contains information about {', '.join(unique_entities)}"
                else:
                    # Too many entities, keep it general
                    description = f"Contains various information related to {clean_name}"
        
        # Add details about what kind of data the table contains
        details = []
        if has_id:
            details.append("identifiers")
        if has_name:
            details.append("names")
        if has_date:
            details.append("dates")
        if has_amount:
            details.append("amounts")
        if has_status:
            details.append("status information")
        
        if details:
            if len(details) <= 3:
                description += f" including {', '.join(details)}"
            else:
                # Too many details, simplify
                description += " including various attributes and metrics"
        
        return description
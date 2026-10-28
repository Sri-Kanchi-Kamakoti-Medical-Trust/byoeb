import os
from uuid import uuid4
from azure.data.tables import TableServiceClient, TableClient
from azure.core.exceptions import ResourceExistsError

# Records that can't be keyed normally are kept here instead of being guessed
# into a partition. onboard_users.py does not know this unit, so they are never
# messaged; they stay in the table for review.
QUARANTINE_PARTITION = "QUARANTINE"


class PatientTable():

    def __init__(self):
        self.table_name = "PatientData"
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.table_service = TableServiceClient.from_connection_string(conn_str=self.connection_string)
        self.table_client = self.table_service.create_table_if_not_exists(self.table_name)


    def insert_data(self, entity):
        """Insert data into the table.

        Returns "inserted", "already_exists", "quarantined" or "failed".
        """
        mrd = str(entity.get('MRD') or '').strip()
        phone = str(entity.get('phone_number') or '').strip()
        unit = mrd.split('/')[0].strip() if '/' in mrd else ''

        if unit and phone:
            entity['PartitionKey'] = unit
            entity['RowKey'] = phone
            status = "inserted"
        else:
            entity['PartitionKey'] = QUARANTINE_PARTITION
            entity['RowKey'] = phone or uuid4().hex
            entity['quarantine_reason'] = "missing_phone" if not phone else ("missing_mrd" if not mrd else "mrd_without_unit")
            status = "quarantined"
        try:
            self.table_client.create_entity(entity=entity)
            return status
        except ResourceExistsError:
            print("Entity already exists.")
            return "already_exists"
        except Exception as e:
            print(f"Error inserting entity: {e}")
            return "failed"

    def fetch_all_rows(self):
        """Fetch all rows from the table."""
        entities = self.table_client.list_entities()
        return list(entities)
    
    def delete_entity(self, partition_key, row_key):
        """Delete an entity from the table."""
        try:
            self.table_client.delete_entity(partition_key=partition_key, row_key=row_key)
            print(f"Entity deleted: PartitionKey={partition_key}")
        except Exception as e:
            print(f"Error deleting entity: {e}")

class DoctorAlternateTable():

    def __init__(self):
        self.table_name = "DoctorAlternateData"
        self.connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.table_service = TableServiceClient.from_connection_string(conn_str=self.connection_string)
        self.table_client = self.table_service.create_table_if_not_exists(self.table_name)

    def insert_data(self, phone_number_primary, phone_number_alternate, org_id):
        """Insert data into the table."""
        entity = {
            "phone_number_primary": phone_number_primary,
            "phone_number_alternate": phone_number_alternate,
            "org_id": org_id
        }
        entity['PartitionKey'] = org_id
            
        entity['RowKey'] = phone_number_primary
        try:
            self.table_client.create_entity(entity=entity)
        except ResourceExistsError:
            print("Entity already exists.")
        except Exception as e:
            print(f"Error inserting entity: {e}")

    def fetch_all_rows(self):
        """Fetch all rows from the table."""
        entities = self.table_client.list_entities()
        return list(entities)

if __name__ == "__main__":
    patient_table = PatientTable()
    row = {
        "MRD": "SEHBLR/123454321",
        "name": "John Doe",
        "phone_number": "1234567890",
        "surgery_name": "CATARACT",
        "suregery_group_name": "Others",
        "age": 37,
        "gender": "male",
        "procedure_type": "Major Procedure",
        "surgery_date": "20-12-2024",
        "operating_doctor": "Hehe",
        "operating_doctor_number": "9987654321",
        "counsellor_name": "MSR counsellor",
        "counsellor_number": ""
    }

    patient_table.insert_data(row)
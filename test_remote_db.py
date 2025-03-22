"""
Script to test the SQLite Cloud remote database connection.
"""
from src.db.database_remote import get_db_connection, execute_query, create_tables

def test_connection():
    """
    Test the connection to the SQLite Cloud database.
    """
    print("Testing connection to SQLite Cloud database...")
    try:
        conn = get_db_connection()
        print("Connection successful!")
        conn.close()
        return True
    except Exception as e:
        print(f"Error connecting to SQLite Cloud: {str(e)}")
        return False

def list_tables():
    """
    List all tables in the SQLite Cloud database.
    """
    print("\nListing tables in the database:")
    try:
        # Query to get all table names
        query = "SELECT name FROM sqlite_master WHERE type='table'"
        result = execute_query(query)
        
        if result is not None and not result.empty:
            for table in result['name']:
                print(f"- {table}")
        else:
            print("No tables found in the database")
    except Exception as e:
        print(f"Error listing tables: {str(e)}")

def main():
    """
    Main function to test the SQLite Cloud database.
    """
    # Test connection
    if not test_connection():
        return
    
    # List tables
    list_tables()
    
    # Create tables if they don't exist
    print("\nCreating tables if they don't exist...")
    create_tables()
    
    # List tables again to verify
    list_tables()
    
    print("\nRemote database test complete!")

if __name__ == "__main__":
    main()

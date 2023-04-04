from src.extraction_lambda.secret_manager.create_entry import create_entry
from src.extraction_lambda.secret_manager.retrieve_entry import retrieve_entry
from src.extraction_lambda.secret_manager.delete_entry import delete_entry
from src.extraction_lambda.secret_manager.list_entries import list_secrets

prompt = """Please specify [e]ntry, [r]etrieval, [d]eletion, [l]isting,
 or e[x]it: \n"""

while True:
    action = input(prompt)

    try:
        respone = ""
        if action == "e":
            secret_id = input("Secret identifier:\n")
            host = input("Host:\n")
            port = input("Port:\n")
            user = input("User:\n")
            password = input("Password:\n")
            database = input("Database:\n")

            response = create_entry(
                secret_id, host, port, user, password, database)

        elif action == "r":
            secret_id = input("Secret identifier:\n")
            entry = retrieve_entry(secret_id)

            respone = "Secrets stored in local file secrets.txt"
        elif action == "d":
            secret_id = input("Specify the secret you would like to delete:")
            delete_entry(secret_id)
        elif action == "l":
            response = list_secrets()

        elif action == "x":
            break
        else:
            raise Exception("Invalid input.")

        print(response)

    except Exception as e:
        print(e)

print("Thank you. Goodbye.")

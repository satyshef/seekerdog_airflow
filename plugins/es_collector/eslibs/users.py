# Под удаление. Использется в es_operator
def extract_users(messages):
    users = []
    for msg in messages:
        username = msg['sender']['username']
        if (username != '') and (username not in users):
            users.append(username)

    print("USERS", users)
    return users
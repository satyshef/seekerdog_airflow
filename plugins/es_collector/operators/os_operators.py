from airflow.decorators import task

@task.python
def save_list_to_file(filename, items):
    if items == None:
        raise "Empty user list"
    
    with open(filename, "w") as file:
        for item in items:
            file.write(item + "\n")
    return True
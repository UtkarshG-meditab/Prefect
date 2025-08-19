@flow
def my_flow(name: str = "World"):
    with open("output.txt", "a") as f:
        f.write(f"Hello, {name}!\n")
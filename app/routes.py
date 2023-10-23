from app import app
from flask import jsonify, request

# Route to get ID
@app.route('/getID', methods=['GET'])
def get_tasks():
    return jsonify(123)

# # Route to create a new task
# @app.route('/tasks', methods=['POST'])
# def create_task():
#     data = request.get_json()
#     # Generate a unique task ID (you might want to use a more robust method)
#     task_id = generate_unique_id()
#     session.execute(
#         """
#         INSERT INTO tasks (id, title, done)
#         VALUES (%s, %s, %s)
#         """,
#         (task_id, data['title'], False)
#     )
#     return jsonify(message='Task created successfully!'), 201

# # Route to get all tasks
# @app.route('/tasks', methods=['GET'])
# def get_tasks():
#     rows = session.execute("SELECT * FROM tasks")
#     tasks = [{'id': row.id, 'title': row.title, 'done': row.done} for row in rows]
#     return jsonify(tasks=tasks)

# # Route to update a task by ID
# @app.route('/tasks/<int:task_id>', methods=['PUT'])
# def update_task(task_id):
#     data = request.get_json()
#     session.execute(
#         """
#         UPDATE tasks SET title = %s, done = %s WHERE id = %s
#         """,
#         (data['title'], data['done'], task_id)
#     )
#     return jsonify(message='Task updated successfully!')

# # Route to delete a task by ID
# @app.route('/tasks/<int:task_id>', methods=['DELETE'])
# def delete_task(task_id):
#     session.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
#     return jsonify(message='Task deleted successfully!')

# # Function to generate a unique task ID (you might want to use a more robust method)
# def generate_unique_id():
#     # Logic to generate a unique ID (for example, using UUID)
#     pass

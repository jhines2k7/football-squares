# Project README

This project is a backend service that manages game data, player information, and scoring events. It uses a variety of technologies to provide a robust and scalable solution.

## Technologies Used

*   **Python:** The primary programming language for the backend logic.
*   **Flask:** A lightweight web framework for creating the API endpoints.
*   **Cosmos DB:** A database service for storing and retrieving game data.
*   **WebSockets:** For real-time communication of game events to clients.

## Project Structure

The project is structured as follows:

*   `models.py`: Defines the data models used in the project.
*   `server.py`: Contains the main application logic, including API endpoints and event handling.
*   `postman/`: Contains schema files for API documentation.
*   `scoring_plays.json`: Example data for scoring plays.
*   `requirements.txt`: Lists the project's dependencies.

## Getting Started

To get started with the project, follow these steps:

1.  Install the required dependencies using `pip install -r requirements.txt`.
2.  Configure the necessary environment variables for database access and other services.
3.  Run the `server.py` file to start the backend service.

## API Endpoints

The project exposes several API endpoints for managing game data, player information, and scoring events. Please refer to the schema files in the `postman/` directory for detailed information about the API endpoints.

## Contributing

Contributions to the project are welcome. Please follow the standard git workflow for submitting pull requests.

## License

This project is licensed under the MIT License.

import uvicorn
import os
from dotenv import load_dotenv
from .models import init_db

load_dotenv()

def main():

    init_db()
    
    # Start the FastAPI
    uvicorn.run(
        "datapulse.api:app",
        host=os.getenv("API_HOST", "0.0.0.0"),
        port=int(os.getenv("API_PORT", 8000)),
        reload=True
    )

if __name__ == "__main__":
    main() 
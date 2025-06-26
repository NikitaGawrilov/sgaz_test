import uuid
import uvicorn
import os
import enum
from fastapi import FastAPI, UploadFile, Form, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import os
from typing import Dict
from pydantic import BaseModel
from loguru import logger

app = FastAPI()

tasks: Dict[str, Dict] = {}

class Status(enum.Enum):
    success = 'success'
    pending = 'pending'
    failed = 'failed'


class TaskStatus(BaseModel):
    task_id: str
    status: str
    error: str | None = None


UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

def process_file(task_id: str, file_path: str):
    try:
        df = pd.read_excel(file_path)
        
        df = df.fillna('')
        df["ID Материала"] = df["ID Материала"].astype('str', errors='ignore').replace(regex='I', value='1').astype('int', errors='raise')
        df['Кол-во по заявке'] = df['Кол-во по заявке'].astype('str', errors='ignore').replace(regex='\D', value='').astype('float')
        df = df[df["Кол-во по заявке"] > df['Поступило всего']]
        df['Расхождение заявка-приход'] = df['Кол-во по заявке'] - df["Поступило всего"]

        result_path = os.path.join(UPLOAD_DIR, f"result_{task_id}.xlsx")
        df.to_excel(result_path)
        
        tasks[task_id]["status"] = Status.success
        tasks[task_id]["result_path"] = result_path
    except Exception as e:
        tasks[task_id]["status"] = Status.failed
        tasks[task_id]["error"] = str(e)
        logger.exception(e)
    finally:
        os.remove(os.path.join(UPLOAD_DIR, f"upload_{task_id}.xlsx"))


@app.post("/upload")
async def upload_file(file: UploadFile, background_tasks: BackgroundTasks):
    task_id = str(uuid.uuid4())

    file_path = os.path.join(UPLOAD_DIR, f"upload_{task_id}.xlsx")
    with open(file_path, "wb") as f:
        f.write(await file.read())

    tasks[task_id] = {
        "status": Status.pending.value,
        "file_path": file_path,
        "result_path": None,
        "error": None
    }

    background_tasks.add_task(process_file, task_id, file_path)
    
    return {"task_id": task_id}

@app.get("/status/{task_id}", response_model=TaskStatus)
async def get_status(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return {
        "task_id": task_id,
        "status": tasks[task_id]["status"],
        "error": tasks[task_id]["error"]
    }

@app.get("/result/{task_id}")
async def get_result(task_id: str):
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Задача не найдена")
    
    if tasks[task_id]["status"] != Status.success:
        raise HTTPException(status_code=404, detail="Задача не завершена")
    
    result_path = tasks[task_id]["result_path"]
    return FileResponse(
        result_path,
        filename=f"result_{task_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
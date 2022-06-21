from fastapi import APIRouter, UploadFile

router = APIRouter()


@router.post("/upload_departments/")
async def root(file: UploadFile):
    contents = await file.read()
    print(contents)
    return 'Valid :D'

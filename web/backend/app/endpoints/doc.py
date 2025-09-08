from fastapi import APIRouter


api_router = APIRouter(prefix="/doc", tags=["doc"])

@api_router.get("")
async def test_root():
    return {"message": "Test endpoint working"}
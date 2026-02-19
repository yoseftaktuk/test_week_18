from fastapi import APIRouter, UploadFile, HTTPException
import json
router = APIRouter()
from dal import MongQury
mongo = MongQury()


@router.get('/analytics/alerts-by-border-and-priority')
def get_alerts_by_border_and_priority():
    return mongo.get_by_border()


@router.get('/analytics/top-urgent-zones')
def get_top_urgent_zones():
    return mongo.get_top_urgent_zones() 


@router.get('/analytics/distance-distribution')
def get_distance_distribution():
    return mongo.distance_distribution()


@router.get('/analytics/low-visibility-high-activity')
def get_low_visibility_high_activity():
    return mongo.analytics_low_visibility_high_activity()


# @router.get('/analytics/hot-zones')
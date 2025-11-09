import os
import motor.motor_asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

class Database:
    def __init__(self):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(os.getenv("MONGO_URI", "mongodb+srv://RahulPrince720:Q7qg69E1oH30LT6d@cluster0.fb0ldjk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"))
        self.db = self.client.subtitle_bot
        self.users = self.db.users
        self.admins = self.db.admins
        self.tasks = self.db.tasks
        self.settings = self.db.settings

    async def is_user_exist(self, user_id: int) -> bool:
        """Check if user exists in database"""
        user = await self.users.find_one({"user_id": user_id})
        return bool(user)

    async def add_user(self, user_id: int, first_name: str, username: str = None) -> bool:
        """Add new user to database"""
        if await self.is_user_exist(user_id):
            return False
        
        user_data = {
            "user_id": user_id,
            "first_name": first_name,
            "username": username,
            "joined_date": datetime.now(),
            "is_banned": False,
            "is_paid": False,
            "paid_until": None,
            "daily_tasks": 0,
            "last_task_date": None,
            "upload_mode": "video",  # video or file
            "screenshot_mode": True,
            "split_mode": False
        }
        
        await self.users.insert_one(user_data)
        return True

    async def get_user(self, user_id: int) -> Optional[Dict[Any, Any]]:
        """Get user data"""
        return await self.users.find_one({"user_id": user_id})

    async def ban_user(self, user_id: int) -> bool:
        """Ban user"""
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"is_banned": True}}
        )
        return result.modified_count > 0

    async def unban_user(self, user_id: int) -> bool:
        """Unban user"""
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"is_banned": False}}
        )
        return result.modified_count > 0

    async def is_user_banned(self, user_id: int) -> bool:
        """Check if user is banned"""
        user = await self.get_user(user_id)
        return user.get("is_banned", False) if user else False

    async def add_paid_user(self, user_id: int, duration_days: int) -> bool:
        """Make user paid for specified duration"""
        paid_until = datetime.now() + timedelta(days=duration_days)
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"is_paid": True, "paid_until": paid_until}}
        )
        return result.modified_count > 0

    async def remove_paid_user(self, user_id: int) -> bool:
        """Remove paid status from user"""
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"is_paid": False, "paid_until": None}}
        )
        return result.modified_count > 0

    async def is_user_paid(self, user_id: int) -> bool:
        """Check if user is paid and subscription is active"""
        user = await self.get_user(user_id)
        if not user or not user.get("is_paid", False):
            return False
        
        paid_until = user.get("paid_until")
        if paid_until and paid_until > datetime.now():
            return True
        else:
            # Subscription expired, remove paid status
            await self.remove_paid_user(user_id)
            return False

    async def can_use_bot(self, user_id: int) -> tuple[bool, str]:
        """Check if user can use bot (only checks if user is not banned)"""
        user = await self.get_user(user_id)
        if not user:
            return False, "User not found"
        
        if user.get("is_banned", False):
            return False, "You are banned from using this bot"
        
        return True, "Bot access granted"

    async def can_start_task(self, user_id: int) -> tuple[bool, str]:
        """Check if user can start a task (checks ban status and task limits)"""
        user = await self.get_user(user_id)
        if not user:
            return False, "User not found"
        
        if user.get("is_banned", False):
            return False, "You are banned from using this bot"
        
        # Check if user is paid
        if await self.is_user_paid(user_id):
            return True, "unlimited"
        
        # Check daily task limit for free users
        today = datetime.now().date()
        last_task_date = user.get("last_task_date")
        daily_tasks = user.get("daily_tasks", 0)
        
        # Reset daily tasks if it's a new day
        if not last_task_date or last_task_date.date() < today:
            await self.users.update_one(
                {"user_id": user_id},
                {"$set": {"daily_tasks": 0, "last_task_date": datetime.now()}}
            )
            daily_tasks = 0
        
        if daily_tasks >= 10:
            return False, "Daily limit reached (10 tasks per day). Upgrade to paid for unlimited access."
        
        return True, f"Tasks remaining: {10 - daily_tasks}"

    async def can_use_task(self, user_id: int) -> tuple[bool, str]:
        """Alias for can_start_task for better naming consistency"""
        return await self.can_start_task(user_id)

    async def increment_user_task(self, user_id: int) -> bool:
        """Increment user's daily task count"""
        today = datetime.now()
        result = await self.users.update_one(
            {"user_id": user_id},
            {
                "$inc": {"daily_tasks": 1},
                "$set": {"last_task_date": today}
            }
        )
        return result.modified_count > 0

    async def get_user_task_info(self, user_id: int) -> Dict[str, Any]:
        """Get user's task-related information"""
        user = await self.get_user(user_id)
        if not user:
            return {}
        
        is_paid = await self.is_user_paid(user_id)
        
        if is_paid:
            return {
                "is_paid": True,
                "daily_tasks": "unlimited",
                "tasks_remaining": "unlimited",
                "last_task_date": user.get("last_task_date")
            }
        
        today = datetime.now().date()
        last_task_date = user.get("last_task_date")
        daily_tasks = user.get("daily_tasks", 0)
        
        # Reset daily tasks if it's a new day
        if not last_task_date or last_task_date.date() < today:
            daily_tasks = 0
        
        return {
            "is_paid": False,
            "daily_tasks": daily_tasks,
            "tasks_remaining": max(0, 10 - daily_tasks),
            "last_task_date": last_task_date,
            "daily_limit": 10
        }

    async def reset_user_daily_tasks(self, user_id: int) -> bool:
        """Reset user's daily task count (admin function)"""
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {"daily_tasks": 0, "last_task_date": datetime.now()}}
        )
        return result.modified_count > 0

    async def get_user_settings(self, user_id: int) -> Dict[str, Any]:
        """Get user settings"""
        user = await self.get_user(user_id)
        if not user:
            return {}
        
        return {
            "upload_mode": user.get("upload_mode", "video"),
            "screenshot_mode": user.get("screenshot_mode", True),
            "split_mode": user.get("split_mode", False)
        }

    async def update_user_setting(self, user_id: int, setting: str, value: Any) -> bool:
        """Update user setting"""
        result = await self.users.update_one(
            {"user_id": user_id},
            {"$set": {setting: value}}
        )
        return result.modified_count > 0

    async def get_all_users(self) -> list:
        """Get all users for broadcast"""
        cursor = self.users.find({}, {"user_id": 1})
        return await cursor.to_list(length=None)

    async def get_users_count(self) -> int:
        """Get total users count"""
        return await self.users.count_documents({})

    async def get_paid_users_count(self) -> int:
        """Get paid users count"""
        return await self.users.count_documents({
            "is_paid": True,
            "paid_until": {"$gt": datetime.now()}
        })

    async def is_admin(self, user_id: int) -> bool:
        """Check if user is admin"""
        admin = await self.admins.find_one({"user_id": user_id})
        return bool(admin)

    async def add_admin(self, user_id: int) -> bool:
        """Add admin"""
        if await self.is_admin(user_id):
            return False
        
        await self.admins.insert_one({
            "user_id": user_id,
            "added_date": datetime.now()
        })
        return True

    async def remove_admin(self, user_id: int) -> bool:
        """Remove admin"""
        result = await self.admins.delete_one({"user_id": user_id})
        return result.deleted_count > 0

    async def get_stats(self) -> Dict[str, int]:
        """Get bot statistics"""
        total_users = await self.get_users_count()
        paid_users = await self.get_paid_users_count()
        banned_users = await self.users.count_documents({"is_banned": True})
        
        return {
            "total_users": total_users,
            "paid_users": paid_users,
            "free_users": total_users - paid_users,
            "banned_users": banned_users
        }

    async def get_task_stats(self) -> Dict[str, int]:
        """Get task-related statistics"""
        today = datetime.now().date()
        
        # Count users who have used tasks today
        users_with_tasks_today = await self.users.count_documents({
            "last_task_date": {"$gte": datetime.combine(today, datetime.min.time())},
            "daily_tasks": {"$gt": 0}
        })
        
        # Count total tasks completed today
        pipeline = [
            {"$match": {
                "last_task_date": {"$gte": datetime.combine(today, datetime.min.time())},
                "daily_tasks": {"$gt": 0}
            }},
            {"$group": {"_id": None, "total_tasks": {"$sum": "$daily_tasks"}}}
        ]
        
        result = await self.users.aggregate(pipeline).to_list(length=1)
        total_tasks_today = result[0]["total_tasks"] if result else 0
        
        return {
            "users_with_tasks_today": users_with_tasks_today,
            "total_tasks_today": total_tasks_today
        }

    async def cleanup_expired_subscriptions(self):
        """Clean up expired paid subscriptions"""
        await self.users.update_many(
            {"paid_until": {"$lt": datetime.now()}},
            {"$set": {"is_paid": False, "paid_until": None}}
        )

db = Database()

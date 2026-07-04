import asyncio
import logging
from sqlalchemy import select
from app.database import AsyncSessionLocal
from app.models.organization import Organization
from app.models.user import User, UserRole
from app.models.project import Project
from app.models.queue import Queue
from app.services.auth import hash_password

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scheduler.seed")

async def seed_data():
    logger.info("Starting database seeding...")
    async with AsyncSessionLocal() as session:
        async with session.begin():
            # Check if user already exists
            existing_user = await session.execute(
                select(User).where(User.email == "admin@scheduler.xyz")
            )
            user = existing_user.scalar_one_or_none()
            if user:
                logger.info("Demo user already exists. Skipping seed.")
                return

            # Create organization
            logger.info("Creating demo organization Acme Corporation...")
            org = Organization(name="Acme Corporation")
            session.add(org)
            await session.flush()

            # Create user
            logger.info("Creating demo admin user admin@scheduler.xyz...")
            user = User(
                org_id=org.id,
                email="admin@scheduler.xyz",
                hashed_password=hash_password("AdminPassword123!"),
                role=UserRole.ADMIN,
            )
            session.add(user)
            await session.flush()

            # Create production workload project
            logger.info("Creating project 'Video Processing Cluster'...")
            project = Project(
                org_id=org.id,
                name="Video Processing Cluster"
            )
            session.add(project)
            await session.flush()

            # Create demo queues
            logger.info("Creating demo queues 'default-queue' & 'critical-queue'...")
            default_queue = Queue(
                project_id=project.id,
                name="default-queue",
                priority=10,
                concurrency_limit=3,
                retry_policy_id="a0000000-0000-0000-0000-000000000001" # Default Fixed
            )
            critical_queue = Queue(
                project_id=project.id,
                name="critical-queue",
                priority=100,
                concurrency_limit=5,
                retry_policy_id="a0000000-0000-0000-0000-000000000003" # Default Exponential
            )
            session.add_all([default_queue, critical_queue])
            
            logger.info("Database seeding successfully completed!")

if __name__ == "__main__":
    asyncio.run(seed_data())

from web3 import Web3, utils
import time
import json
import os
import threading

infura_url = 'https://arbitrum-mainnet.infura.io/v3/9cacf19f33fc4091b97346072af54cdc'
w3 = Web3(Web3.HTTPProvider(infura_url))

erc20_abi = """[
        {
            "anonymous": false,
            "inputs": [
                {
                    "indexed": true,
                    "name": "from",
                    "type": "address"
                },
                {
                    "indexed": true,
                    "name": "to",
                    "type": "address"
                },
                {
                    "indexed": false,
                    "name": "value",
                    "type": "uint256"
                }
            ],
            "name": "Transfer",
            "type": "event"
        }
    ]
    """


contract_abi = '[{"inputs":[{"internalType":"address[]","name":"hotAddresses","type":"address[]"},{"internalType":"address[]","name":"coldAddresses","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"},{"internalType":"address","name":"usdcAddress","type":"address"},{"internalType":"uint64","name":"_disputePeriodSeconds","type":"uint64"},{"internalType":"uint64","name":"_blockDurationMillis","type":"uint64"},{"internalType":"uint64","name":"_lockerThreshold","type":"uint64"}],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"newBlockDurationMillis","type":"uint64"}],"name":"ChangedBlockDurationMillis","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"newDisputePeriodSeconds","type":"uint64"}],"name":"ChangedDisputePeriodSeconds","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"newLockerThreshold","type":"uint64"}],"name":"ChangedLockerThreshold","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"uint64","name":"usd","type":"uint64"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"uint64","name":"usd","type":"uint64"},{"indexed":false,"internalType":"uint32","name":"errorCode","type":"uint32"}],"name":"FailedPermitDeposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"bytes32","name":"message","type":"bytes32"},{"indexed":false,"internalType":"uint32","name":"errorCode","type":"uint32"}],"name":"FailedWithdrawal","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"epoch","type":"uint64"},{"indexed":false,"internalType":"bytes32","name":"hotValidatorSetHash","type":"bytes32"},{"indexed":false,"internalType":"bytes32","name":"coldValidatorSetHash","type":"bytes32"}],"name":"FinalizedValidatorSetUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"address","name":"destination","type":"address"},{"indexed":false,"internalType":"uint64","name":"usd","type":"uint64"},{"indexed":false,"internalType":"uint64","name":"nonce","type":"uint64"},{"indexed":false,"internalType":"bytes32","name":"message","type":"bytes32"}],"name":"FinalizedWithdrawal","type":"event"},{"anonymous":false,"inputs":[{"components":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"destination","type":"address"},{"internalType":"uint64","name":"usd","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"internalType":"uint64","name":"requestedTime","type":"uint64"},{"internalType":"uint64","name":"requestedBlockNumber","type":"uint64"},{"internalType":"bytes32","name":"message","type":"bytes32"}],"indexed":false,"internalType":"struct Withdrawal","name":"withdrawal","type":"tuple"}],"name":"InvalidatedWithdrawal","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"finalizer","type":"address"},{"indexed":false,"internalType":"bool","name":"isFinalizer","type":"bool"}],"name":"ModifiedFinalizer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"locker","type":"address"},{"indexed":false,"internalType":"bool","name":"isLocker","type":"bool"}],"name":"ModifiedLocker","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Paused","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint64","name":"epoch","type":"uint64"},{"indexed":false,"internalType":"bytes32","name":"hotValidatorSetHash","type":"bytes32"},{"indexed":false,"internalType":"bytes32","name":"coldValidatorSetHash","type":"bytes32"},{"indexed":false,"internalType":"uint64","name":"updateTime","type":"uint64"}],"name":"RequestedValidatorSetUpdate","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"user","type":"address"},{"indexed":false,"internalType":"address","name":"destination","type":"address"},{"indexed":false,"internalType":"uint64","name":"usd","type":"uint64"},{"indexed":false,"internalType":"uint64","name":"nonce","type":"uint64"},{"indexed":false,"internalType":"bytes32","name":"message","type":"bytes32"},{"indexed":false,"internalType":"uint64","name":"requestedTime","type":"uint64"}],"name":"RequestedWithdrawal","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"account","type":"address"}],"name":"Unpaused","type":"event"},{"inputs":[{"components":[{"internalType":"address","name":"user","type":"address"},{"internalType":"uint64","name":"usd","type":"uint64"},{"internalType":"uint64","name":"deadline","type":"uint64"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature","name":"signature","type":"tuple"}],"internalType":"struct DepositWithPermit[]","name":"deposits","type":"tuple[]"}],"name":"batchedDepositWithPermit","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32[]","name":"messages","type":"bytes32[]"}],"name":"batchedFinalizeWithdrawals","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"destination","type":"address"},{"internalType":"uint64","name":"usd","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"internalType":"struct WithdrawalRequest[]","name":"withdrawalRequests","type":"tuple[]"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"hotValidatorSet","type":"tuple"}],"name":"batchedRequestWithdrawals","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"blockDurationMillis","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint64","name":"newBlockDurationMillis","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeColdValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"changeBlockDurationMillis","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint64","name":"newDisputePeriodSeconds","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeColdValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"changeDisputePeriodSeconds","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint64","name":"newLockerThreshold","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeColdValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"changeLockerThreshold","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"coldValidatorSetHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"disputePeriodSeconds","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"hotAddresses","type":"address[]"},{"internalType":"address[]","name":"coldAddresses","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSetUpdateRequest","name":"newValidatorSet","type":"tuple"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeColdValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"},{"internalType":"uint64","name":"nonce","type":"uint64"}],"name":"emergencyUnlock","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"epoch","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"finalizeValidatorSetUpdate","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"finalizedWithdrawals","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"finalizers","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"getLockersVotingLock","outputs":[{"internalType":"address[]","name":"","type":"address[]"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"hotValidatorSetHash","outputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32[]","name":"messages","type":"bytes32[]"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeColdValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"invalidateWithdrawals","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"locker","type":"address"}],"name":"isVotingLock","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"lockerThreshold","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"","type":"address"}],"name":"lockers","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"finalizer","type":"address"},{"internalType":"bool","name":"_isFinalizer","type":"bool"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"modifyFinalizer","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"locker","type":"address"},{"internalType":"bool","name":"_isLocker","type":"bool"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"modifyLocker","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"nValidators","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"paused","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"pendingValidatorSetUpdate","outputs":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"uint64","name":"totalValidatorPower","type":"uint64"},{"internalType":"uint64","name":"updateTime","type":"uint64"},{"internalType":"uint64","name":"updateBlockNumber","type":"uint64"},{"internalType":"uint64","name":"nValidators","type":"uint64"},{"internalType":"bytes32","name":"hotValidatorSetHash","type":"bytes32"},{"internalType":"bytes32","name":"coldValidatorSetHash","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"requestedWithdrawals","outputs":[{"internalType":"address","name":"user","type":"address"},{"internalType":"address","name":"destination","type":"address"},{"internalType":"uint64","name":"usd","type":"uint64"},{"internalType":"uint64","name":"nonce","type":"uint64"},{"internalType":"uint64","name":"requestedTime","type":"uint64"},{"internalType":"uint64","name":"requestedBlockNumber","type":"uint64"},{"internalType":"bytes32","name":"message","type":"bytes32"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalValidatorPower","outputs":[{"internalType":"uint64","name":"","type":"uint64"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"unvoteEmergencyLock","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"hotAddresses","type":"address[]"},{"internalType":"address[]","name":"coldAddresses","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSetUpdateRequest","name":"newValidatorSet","type":"tuple"},{"components":[{"internalType":"uint64","name":"epoch","type":"uint64"},{"internalType":"address[]","name":"validators","type":"address[]"},{"internalType":"uint64[]","name":"powers","type":"uint64[]"}],"internalType":"struct ValidatorSet","name":"activeHotValidatorSet","type":"tuple"},{"components":[{"internalType":"uint256","name":"r","type":"uint256"},{"internalType":"uint256","name":"s","type":"uint256"},{"internalType":"uint8","name":"v","type":"uint8"}],"internalType":"struct Signature[]","name":"signatures","type":"tuple[]"}],"name":"updateValidatorSet","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"usdcToken","outputs":[{"internalType":"contract ERC20Permit","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"usedMessages","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"voteEmergencyLock","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"withdrawalsInvalidated","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"}]'


class EventListener:
    def __init__(self):
        self.contract_address = w3.to_checksum_address('0x2df1c51e09aecf9cacb7bc98cb1742757f163df7')
        self.erc20_address = w3.to_checksum_address("0xaf88d065e77c8cC2239327C5EDb3A432268e5831")
        self.contract = w3.eth.contract(abi=contract_abi, address=self.contract_address)
        self.stream_thread = None
        self.streaming = False
        self.current_block = 0
        self.step = 0

    def watch_withdrawals(self, current_block):
        withdrawal_logs = self.contract.events.FinalizedWithdrawal().getLogs(fromBlock=current_block)    
        withdrawal_data = []
                
        for log in withdrawal_logs:
            print(log)
            log_entry = {
                "event": log.get('event'),
                "user": log.get('args').get('user'),
                "amount": log.get('args').get('usd') / 1000000,
                "transactionHash": '0x' + log.get('transactionHash').hex()
            }
            withdrawal_data.append(log_entry)   
            
      
        if len(withdrawal_data) > 0:
            print('Withdrawals:', withdrawal_data)
    
        if not os.path.exists("withdrawals.json"):
            with open("withdrawals.json", "w") as file:
                json.dump([], file)

        with open("withdrawals.json", "r+") as file:
            data = json.load(file)
            data.extend(withdrawal_data)
            file.seek(0)
            json.dump(data, file, indent=4)
            
    def watch_transfers(self, current_block):
        transfers_data = []
        transfer_event_signature = w3.keccak(text="Transfer(address,address,uint256)").hex()
        transfer_event_signature = "0x" + transfer_event_signature # ensure it starts with 0x
        if not transfer_event_signature.startswith("0x"):
            raise ValueError("Transfer event signature must start with 0x")

        def pad_address_to_32_bytes(address):
            address_bytes = w3.to_bytes(hexstr=address)
            return w3.to_hex(address_bytes.rjust(32, b'\x00'))

        target_address_topic = pad_address_to_32_bytes(self.contract_address)
        event_filter = {
            "fromBlock": current_block-self.step,
            "toBlock": current_block,
            "address": self.erc20_address,
            "topics": [
                transfer_event_signature,
                None,  # from address
                target_address_topic 
            ]
        }

        try:
            logs = w3.eth.get_logs(event_filter)
        except ValueError as e:
            print(f"Error fetching logs: {e}")
            logs = []

        for log in logs:
            print(log)
            log_entry = {
                "event": "Transfer",
                "user": '0x' + log['topics'][1].hex().lstrip('0'),
                "amount": int(log['data'].hex(), 16)/(10**6),
                "transactionHash": '0x' + log['transactionHash'].hex()
            }
            transfers_data.append(log_entry)
            
        batched_deposit_logs=self.contract.events.Deposit().getLogs(fromBlock=current_block)

        for log in batched_deposit_logs:
            log_entry = {
                "event": log.get('event'),
                "user": log.get('args').get('user'),
                "amount": log.get('args').get('usd') / 1000000,
                "transactionHash": '0x' + log.get('transactionHash').hex()
            }
                    
            transfers_data.append(log_entry)  

            
        if len(transfers_data) > 0:
            print('Transfers:', transfers_data)

        if not os.path.exists("transfers.json"):
            with open("transfers.json", "w") as file:
                json.dump([], file)

        with open("transfers.json", "r+") as file:
            data = json.load(file)
            data.extend(transfers_data)
            file.seek(0)
            json.dump(data, file, indent=4)
     
    
    def stream_events(self):
        while self.streaming:
            if self.current_block == 0:
                self.current_block = w3.eth.get_block_number()
            else:
                current_block = w3.eth.get_block_number()
                self.step = current_block - self.current_block
                self.current_block = current_block
            
            print(f"start {self.current_block - self.step}")
            print(f"end {self.current_block}")
            
            self.watch_withdrawals(self.current_block)
            self.watch_transfers(self.current_block)
            time.sleep(1)

    def start_stream(self):
        if not self.streaming:
            self.streaming = True
            self.stream_thread = threading.Thread(target=self.stream_events)
            self.stream_thread.start()
            print("Streaming started...")

    def stop_stream(self):
        if self.streaming:
            self.streaming = False
            if self.stream_thread:
                self.stream_thread.join()  # Wait for thread to finish
            print("Streaming stopped...")

if __name__ == "__main__":
    test = EventListener()
    test.start_stream()
 